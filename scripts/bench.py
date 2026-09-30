#!/usr/bin/env python3
"""Bounded, signed DynamoDB API benchmark against an existing table.

The table must have a string partition key named ``pk`` and may have a sort key.
Credentials come from the normal AWS environment or profile chain.
Install boto3 separately before running this script.
Batch and transaction cases carry two items per request. ``delete_missing``
measures deletion of an absent unique key; it is not an existing-item delete.
"""

import argparse
import json
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config


def percentile(values, fraction):
    ordered = sorted(values)
    return round(ordered[int((len(ordered) - 1) * fraction)], 2)


OPERATIONS = (
    "get",
    "query",
    "scan",
    "batch_get",
    "transact_get",
    "describe_table",
    "list_tables",
    "put",
    "update",
    "delete_missing",
    "batch_write",
    "transact_write",
)


def run_case(client, table, operation, workers, seconds, payload, keys, sort_key):
    gate = threading.Barrier(workers + 1)
    lock = threading.Lock()
    durations = []
    errors = []
    stop = [0.0]

    def worker(worker_id):
        local_durations = []
        local_errors = []
        index = 0
        gate.wait()
        while time.perf_counter() < stop[0]:
            started = time.perf_counter()
            try:
                key = {"pk": {"S": keys[(worker_id + index) % len(keys)]}}
                if sort_key:
                    key[sort_key[0]] = {"N": sort_key[1]}
                unique = (
                    f"bench-{worker_id}-{index}-{uuid.uuid4().hex}"
                    if operation in {"put", "delete_missing", "batch_write", "transact_write"}
                    else None
                )

                def new_item(suffix=""):
                    item = {
                        "pk": {"S": unique + suffix},
                        "payload": {"S": payload},
                    }
                    if sort_key:
                        item[sort_key[0]] = {"N": sort_key[1]}
                    return item

                if operation == "get":
                    response = client.get_item(
                        TableName=table,
                        Key=key,
                        ConsistentRead=True,
                    )
                    if "Item" not in response:
                        raise RuntimeError("seed item missing")
                elif operation == "query":
                    response = client.query(
                        TableName=table,
                        KeyConditionExpression="pk = :pk",
                        ExpressionAttributeValues={":pk": key["pk"]},
                        ConsistentRead=True,
                        Limit=1,
                    )
                    if not response.get("Items"):
                        raise RuntimeError("seed query returned no item")
                elif operation == "scan":
                    response = client.scan(TableName=table, ConsistentRead=True, Limit=1)
                    if not response.get("Items"):
                        raise RuntimeError("seed scan returned no item")
                elif operation == "batch_get":
                    keys_for_batch = []
                    for offset in range(2):
                        item_key = {"pk": {"S": keys[(worker_id + index + offset) % len(keys)]}}
                        if sort_key:
                            item_key[sort_key[0]] = {"N": sort_key[1]}
                        keys_for_batch.append(item_key)
                    response = client.batch_get_item(
                        RequestItems={table: {"Keys": keys_for_batch, "ConsistentRead": True}}
                    )
                    if len(response.get("Responses", {}).get(table, [])) != 2 or response.get(
                        "UnprocessedKeys"
                    ):
                        raise RuntimeError("batch get incomplete")
                elif operation == "transact_get":
                    requests = []
                    for offset in range(2):
                        item_key = {"pk": {"S": keys[(worker_id + index + offset) % len(keys)]}}
                        if sort_key:
                            item_key[sort_key[0]] = {"N": sort_key[1]}
                        requests.append({"Get": {"TableName": table, "Key": item_key}})
                    response = client.transact_get_items(TransactItems=requests)
                    if len(response.get("Responses", [])) != 2 or not all(
                        "Item" in entry for entry in response["Responses"]
                    ):
                        raise RuntimeError("transaction get incomplete")
                elif operation == "describe_table":
                    response = client.describe_table(TableName=table)
                    if response["Table"]["TableStatus"] != "ACTIVE":
                        raise RuntimeError("table is not active")
                elif operation == "list_tables":
                    response = client.list_tables(Limit=100)
                    if table not in response["TableNames"]:
                        raise RuntimeError("table missing from listing")
                elif operation == "put":
                    client.put_item(TableName=table, Item=new_item())
                elif operation == "update":
                    client.update_item(
                        TableName=table,
                        Key=key,
                        UpdateExpression="SET #version = :version",
                        ExpressionAttributeNames={"#version": "version"},
                        ExpressionAttributeValues={":version": {"N": str(index)}},
                    )
                elif operation == "delete_missing":
                    missing_key = {"pk": {"S": unique}}
                    if sort_key:
                        missing_key[sort_key[0]] = {"N": sort_key[1]}
                    client.delete_item(TableName=table, Key=missing_key)
                elif operation == "batch_write":
                    response = client.batch_write_item(
                        RequestItems={
                            table: [
                                {"PutRequest": {"Item": new_item("-a")}},
                                {"PutRequest": {"Item": new_item("-b")}},
                            ]
                        }
                    )
                    if response.get("UnprocessedItems"):
                        raise RuntimeError("batch write incomplete")
                elif operation == "transact_write":
                    client.transact_write_items(
                        TransactItems=[
                            {"Put": {"TableName": table, "Item": new_item("-a")}},
                            {"Put": {"TableName": table, "Item": new_item("-b")}},
                        ]
                    )
                else:
                    raise RuntimeError(f"unknown operation: {operation}")
                local_durations.append((time.perf_counter() - started) * 1000)
            except Exception as error:
                local_errors.append(f"{type(error).__name__}: {error}"[:300])
                break
            index += 1
        with lock:
            durations.extend(local_durations)
            errors.extend(local_errors)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, index) for index in range(workers)]
        started = time.perf_counter()
        stop[0] = started + seconds
        gate.wait()
        for future in futures:
            future.result()
        elapsed = time.perf_counter() - started
    items_per_request = (
        2
        if operation in {"batch_get", "batch_write", "transact_get", "transact_write"}
        else 0 if operation in {"describe_table", "list_tables"} else 1
    )
    return {
        "operation": operation,
        "items_per_request": items_per_request,
        "clients": workers,
        "elapsed_s": round(elapsed, 2),
        "completed": len(durations),
        "errors": len(errors),
        "first_error": errors[0] if errors else None,
        "ops_per_s": round(len(durations) / elapsed, 2),
        "items_per_s": round(len(durations) * items_per_request / elapsed, 2),
        "mean_ms": round(statistics.mean(durations), 2) if durations else None,
        "p50_ms": percentile(durations, 0.50) if durations else None,
        "p95_ms": percentile(durations, 0.95) if durations else None,
        "p99_ms": percentile(durations, 0.99) if durations else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--seed-keys", type=int, default=64)
    parser.add_argument("--sort-key-name", help="optional numeric sort-key attribute")
    parser.add_argument("--sort-key-value", default="1")
    parser.add_argument("--operations", nargs="+", choices=OPERATIONS, default=["get", "put"])
    parser.add_argument("--output", help="write JSON results to this path")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="record every case even if an earlier case has request errors",
    )
    args = parser.parse_args()
    if args.seconds <= 0 or args.payload_bytes <= 0 or args.seed_keys <= 0:
        parser.error("seconds, payload bytes, and seed keys must be positive")
    if any(clients <= 0 for clients in args.clients):
        parser.error("client counts must be positive")
    if args.seed_keys < 2 and {"batch_get", "transact_get"}.intersection(args.operations):
        parser.error("batch_get and transact_get require at least two seed keys")

    client = boto3.client(
        "dynamodb",
        endpoint_url=args.endpoint,
        region_name=args.region,
        config=Config(
            max_pool_connections=max(args.clients),
            connect_timeout=2,
            read_timeout=10,
            retries={"total_max_attempts": 1},
        ),
    )
    description = client.describe_table(TableName=args.table)["Table"]
    expected_schema = [{"AttributeName": "pk", "KeyType": "HASH"}]
    sort_key = None
    if args.sort_key_name:
        expected_schema.append({"AttributeName": args.sort_key_name, "KeyType": "RANGE"})
        sort_key = (args.sort_key_name, args.sort_key_value)
    attribute_types = {
        attribute["AttributeName"]: attribute["AttributeType"]
        for attribute in description["AttributeDefinitions"]
    }
    if description["TableStatus"] != "ACTIVE" or description["KeySchema"] != expected_schema:
        parser.error("table must be ACTIVE with a string pk and the declared numeric sort key")
    if attribute_types.get("pk") != "S" or (
        sort_key and attribute_types.get(sort_key[0]) != "N"
    ):
        parser.error("pk must be a string and the optional sort key must be numeric")
    keys = [f"bench-seed-{index:06}" for index in range(args.seed_keys)]
    payload = "x" * args.payload_bytes
    for key in keys:
        item = {"pk": {"S": key}, "payload": {"S": payload}}
        if sort_key:
            item[sort_key[0]] = {"N": sort_key[1]}
        client.put_item(
            TableName=args.table,
            Item=item,
        )
    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": args.endpoint,
        "table": args.table,
        "seconds_per_case": args.seconds,
        "payload_bytes": args.payload_bytes,
        "seed_keys": args.seed_keys,
        "sort_key": sort_key,
        "strong_reads": True,
        "sdk_retries": 0,
        "operations": args.operations,
        "cases": [],
    }
    for operation in args.operations:
        for workers in args.clients:
            case = run_case(
                client, args.table, operation, workers, args.seconds, payload, keys, sort_key
            )
            results["cases"].append(case)
            print(json.dumps(case), flush=True)
            if args.output:
                with open(args.output, "w", encoding="utf-8") as output:
                    json.dump(results, output, indent=2)
                    output.write("\n")
            if case["errors"] and not args.continue_on_error:
                raise SystemExit("stopped after request error")
    if any(case["errors"] for case in results["cases"]):
        raise SystemExit("one or more cases had request errors")


if __name__ == "__main__":
    main()
