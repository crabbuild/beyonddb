#!/usr/bin/env python3
"""Bounded, signed DynamoDB point-operation benchmark against an existing table.

The table must have a string partition key named ``pk`` and may have a sort key.
Credentials come from the normal AWS environment or profile chain.
Install boto3 separately before running this script.
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
                if operation == "get":
                    key = {"pk": {"S": keys[(worker_id + index) % len(keys)]}}
                    if sort_key:
                        key[sort_key[0]] = {"N": sort_key[1]}
                    response = client.get_item(
                        TableName=table,
                        Key=key,
                        ConsistentRead=True,
                    )
                    if "Item" not in response:
                        raise RuntimeError("seed item missing")
                else:
                    item = {
                        "pk": {"S": f"bench-{worker_id}-{index}-{uuid.uuid4().hex}"},
                        "payload": {"S": payload},
                    }
                    if sort_key:
                        item[sort_key[0]] = {"N": sort_key[1]}
                    client.put_item(
                        TableName=table,
                        Item=item,
                    )
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
    return {
        "operation": operation,
        "clients": workers,
        "elapsed_s": round(elapsed, 2),
        "completed": len(durations),
        "errors": len(errors),
        "first_error": errors[0] if errors else None,
        "ops_per_s": round(len(durations) / elapsed, 2),
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
    parser.add_argument("--output", help="write JSON results to this path")
    args = parser.parse_args()
    if args.seconds <= 0 or args.payload_bytes <= 0 or args.seed_keys <= 0:
        parser.error("seconds, payload bytes, and seed keys must be positive")
    if any(clients <= 0 for clients in args.clients):
        parser.error("client counts must be positive")

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
        "cases": [],
    }
    for operation in ("get", "put"):
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
            if case["errors"]:
                raise SystemExit("stopped after request error")


if __name__ == "__main__":
    main()
