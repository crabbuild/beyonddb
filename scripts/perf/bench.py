#!/usr/bin/env python3
"""Benchmark one table over configurable ordered data cells and bounded nodes.

All measured traffic uses signed boto3 calls with retries disabled. The load is
closed loop: throughput and response latency are measured together, including
completion drain time. It does not estimate an open-loop arrival-rate SLO.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import itertools
import json
import math
import re
from pathlib import Path
import statistics
import subprocess
import threading
import time
import uuid

from control import (ACCOUNT_NS, DATA_NS, Control, data_partition,
                     digest, identity, target_id, tenant)
from topology import Topology

TABLE = "Perf30"


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def key(ordinal, row=0, sk=1, topology=Topology()):
    # Choose a prefix within the installed Unicode PK bootstrap interval.
    return {"pk": {"S": topology.prefix(ordinal) + f"/seed-{row:04d}"},
            "sk": {"N": str(sk)}}


def verify(control, manifest):
    topology = control.topology
    if (manifest.get("topology", Topology().to_dict()) != topology.to_dict()
            or manifest["data_cells"] != topology.cells
            or sorted(row["ordinal"] for row in manifest["ranges"]) != list(range(topology.cells))
            or any(row["node"] != topology.owner(row["ordinal"]) for row in manifest["ranges"])):
        raise RuntimeError("table manifest differs from configured topology")
    for row in manifest["ranges"]:
        spec = row["installed_spec"]
        for sample in range(4):
            value = key(row["ordinal"], sample, topology=topology)["pk"]["S"].encode()
            encoded = b"\x10" + value.replace(b"\0", b"\0\xff") + b"\0\0"
            lower = bytes(spec["lower"]) if spec["lower"] is not None else None
            upper = bytes(spec["upper"]) if spec["upper"] is not None else None
            if (lower is not None and encoded < lower) or (upper is not None and encoded >= upper):
                raise RuntimeError("workload key is outside its installed ordered interval")
    nodes = control.ownership()
    expected = {row["scope"]: topology.node_names[row["node"]] for row in manifest["ranges"]}
    observed = {}
    for node in nodes:
        for cell in node["cells"]:
            if cell["scope"] in expected:
                if cell["scope"] in observed:
                    raise RuntimeError("duplicate data owner")
                observed[cell["scope"]] = node["node"]
                if cell["phase"] != "resident":
                    raise RuntimeError(f"data cell not resident: {cell}")
    if observed != expected:
        raise RuntimeError(f"placement differs from the configured manifest: {Counter(observed.values())}")
    return {"data_cells_per_node": dict(Counter(observed.values())), "nodes": nodes,
            "observed_at": datetime.now(timezone.utc).isoformat()}


def prepare(control, output):
    topology = control.topology
    for i in topology.indices:
        control.action(i, "pause_balancing")
    # Table identity is deterministic from the next account command sequence.
    # No other writer may run against this dedicated fixture during setup.
    sequence, existing = control.query(0, ACCOUNT_NS, bytes(4), 7, TABLE)
    if existing is not None:
        raise RuntimeError("fixture table already exists; use a fresh deployment")
    table_id = (tenant() + digest(b"beyonddb.table.v1\0",
                target_id(ACCOUNT_NS, bytes(4)), (sequence+1).to_bytes(8, "big"),
                TABLE.encode())[:16]).hex()
    manifest = {"table": TABLE, "table_id": table_id, "account_sequence": sequence,
                "ranges": [], "data_cells": topology.cells, "topology": topology.to_dict(), "placement": "controlled first touch",
                "balancing_paused": True, "payload_bytes": 1024}
    for ordinal in range(topology.cells):
        partition = data_partition(table_id, ordinal)
        _, state = control.query(topology.owner(ordinal), DATA_NS, partition, 4, None)
        if state is not None:
            raise RuntimeError("future range unexpectedly installed")
        _, scope = identity(DATA_NS, partition)
        manifest["ranges"].append({"ordinal": ordinal, "node": topology.owner(ordinal),
                                    "partition": partition.hex(), "scope": scope})
        print(f"Warmed range {ordinal:02d} on {topology.node_names[topology.owner(ordinal)]}", flush=True)
    control.sdk().create_table(TableName=TABLE, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "N"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}])
    clients = [control.sdk(i) for i in topology.indices]
    for row in manifest["ranges"]:
        _, state = control.query(row["node"], DATA_NS, bytes.fromhex(row["partition"]), 4, None)
        if state is None or state["spec"]["table"]["id"] != table_id:
            raise RuntimeError("normal table creation did not install the predicted range")
        if int.from_bytes(bytes(state["spec"]["partition_id"]), "big") != row["ordinal"]:
            raise RuntimeError("installed range ordinal differs")
        row["installed_spec"] = state["spec"]
        for sample in range(4):
            for sk in (1, 2):
                clients[row["node"]].put_item(TableName=TABLE, Item={**key(row["ordinal"], sample, sk, topology),
                                                "payload": {"S": "x" * 1024}})
    manifest["seed_items"] = topology.cells * 8
    save(output / "table.json", manifest)
    save(output / "ownership-prepared.json", verify(control, manifest))
    print(f"Verified one table, {topology.cells} ranges, {topology.cells_per_node} data cells per node and {manifest['seed_items']} seed items.", flush=True)


def percentiles(values):
    if not values:
        return {name: None for name in ("mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")}
    ordered = sorted(values)
    result = {f"p{p}_ms": round(ordered[math.ceil(len(ordered)*p/100)-1], 3) for p in (50, 95, 99)}
    return {"mean_ms": round(statistics.mean(values), 3), **result, "max_ms": round(max(values), 3)}


def run_case(clients, operation, workers, seconds, run_id, phase, topology=Topology()):
    if len(clients) != topology.nodes:
        raise ValueError("SDK clients must match configured node count")
    barrier = threading.Barrier(workers + 1)
    stop = [0.0]
    ordinal_counter = itertools.count()
    samples, failures, coverage, attempted, endpoints = [], [], Counter(), Counter(), Counter()
    witnesses = {}
    lock = threading.Lock()

    def worker(worker_id):
        local, errors, successful_cells, attempted_cells, endpoint_counts = [], [], Counter(), Counter(), Counter()
        local_witnesses = {}
        barrier.wait()
        while time.perf_counter() < stop[0]:
            ordinal = next(ordinal_counter)
            cell = ordinal % topology.cells
            # Frontend and owner are independent: rotate ingress across all nodes.
            front = (ordinal // topology.cells + cell) % topology.nodes
            client = clients[front]
            other = topology.partner(cell)
            row = (ordinal // topology.cells) % 4
            touched = [cell, other] if operation.startswith("transact_") else [cell]
            attempted_cells.update(touched)
            endpoint_counts[front] += 1
            started = time.perf_counter()
            try:
                if operation == "get":
                    response = client.get_item(TableName=TABLE, Key=key(cell, row, topology=topology), ConsistentRead=True)
                    if response.get("Item", {}).get("payload", {}).get("S") != "x" * 1024:
                        raise RuntimeError("point-read payload mismatch")
                elif operation == "query":
                    response = client.query(TableName=TABLE, ConsistentRead=True,
                        KeyConditionExpression="pk = :pk AND sk BETWEEN :a AND :b",
                        ExpressionAttributeValues={":pk": key(cell, row, topology=topology)["pk"], ":a": {"N": "1"}, ":b": {"N": "2"}})
                    if len(response.get("Items", [])) != 2:
                        raise RuntimeError("ordered range-query result mismatch")
                elif operation == "put":
                    item = key(cell, row, topology=topology)
                    item["pk"]["S"] = topology.prefix(cell) + f"/write-{run_id}-{phase}-{worker_id}-{ordinal}"
                    client.put_item(TableName=TABLE, Item={**item, "payload": {"S": "x" * 1024}})
                elif operation == "transact_get":
                    response = client.transact_get_items(TransactItems=[
                        {"Get": {"TableName": TABLE, "Key": key(c, row, topology=topology)}} for c in (cell, other)])
                    items = response.get("Responses", [])
                    if len(items) != 2 or not all(i.get("Item", {}).get("payload", {}).get("S") == "x" * 1024 for i in items):
                        raise RuntimeError("transaction-read payload mismatch")
                elif operation == "transact_write":
                    items = []
                    for c in (cell, other):
                        item = key(c, row, topology=topology)
                        item["pk"]["S"] = topology.prefix(c) + f"/tx-{run_id}-{phase}-{worker_id}-{ordinal}"
                        items.append({"Put": {"TableName": TABLE, "Item": {**item, "payload": {"S": "x" * 1024}}}})
                    client.transact_write_items(TransactItems=items, ClientRequestToken=str(uuid.uuid4()))
                else:
                    raise ValueError(operation)
                local.append((time.perf_counter() - started) * 1000)
                successful_cells.update(touched)
                if operation == "put":
                    local_witnesses[cell] = item
                elif operation == "transact_write":
                    for c, entry in zip((cell, other), items):
                        local_witnesses[c] = {k: v for k, v in entry["Put"]["Item"].items() if k in ("pk", "sk")}
            except Exception as error:
                errors.append({"latency_ms": (time.perf_counter() - started)*1000,
                               "error": str(error)[:1200], "cells": touched, "frontend": front})
                # Bounded failure exposure; preserve each uncertain outcome, no retry.
                if len(errors) >= 3:
                    break
        with lock:
            samples.extend(local)
            failures.extend(errors)
            coverage.update(successful_cells)
            attempted.update(attempted_cells)
            endpoints.update(endpoint_counts)
            witnesses.update(local_witnesses)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, i) for i in range(workers)]
        started = time.perf_counter()
        stop[0] = started + seconds
        barrier.wait()
        for future in futures:
            future.result()
        elapsed = time.perf_counter() - started
    count = len(samples)
    multiplicity = 2 if operation in ("query", "transact_get", "transact_write") else 1
    return {"operation": operation, "clients": workers, "phase": phase,
            "requested_s": seconds, "elapsed_s": round(elapsed, 3), "completed": count,
            "errors": len(failures), "requests_per_s": round(count/elapsed, 3),
            "items_per_request": multiplicity, "items_per_s": round(count*multiplicity/elapsed, 3),
            **percentiles(samples), "successful_cell_touches": dict(sorted(coverage.items())),
            "attempted_cell_touches": dict(sorted(attempted.items())),
            "frontend_requests": dict(endpoints), "latency_samples_ms": samples,
            "failures": failures, "write_witnesses": witnesses, "tail_sample_warning": count < 1000,
            "covers_all_cells": set(coverage) == set(range(topology.cells))}


def snapshot_cgroups(topology):
    result = {}
    for i in range(1, topology.nodes + 1):
        result[f"node{i}"] = subprocess.check_output([
            "docker", "--context", "colima-bdbperf30", "exec", f"beyonddb-perf30-node{i}",
            "sh", "-c", "cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.events /sys/fs/cgroup/io.stat; df -B1 /data"], text=True)
    return result


def measure(control, output, args):
    topology = control.topology
    manifest = json.loads((output / "table.json").read_text())
    results = {"started_at": datetime.now(timezone.utc).isoformat(), "endpoints": topology.endpoints,
               "topology": topology.to_dict(), "label": args.label,
               "planned_cases": len(args.operations) * len(args.clients) * args.repeats,
               "warmup_s": args.warmup, "operations": args.operations,
               "profile": "release", "payload_bytes": 1024, "strong_reads": True,
               "sdk_retries": 0, "client_read_timeout_s": 45, "model": "closed_loop",
               "transaction_items": 2, "transaction_placement": "different cells and nodes" if topology.nodes > 1 else "different cells on one node",
               "durability": "bucket publication; follower durability disabled", "warmup_errors": 0, "cases": []}
    clients = [control.sdk(i, max(args.clients)) for i in topology.indices]
    save(output / f"ownership-before-{args.label}.json", verify(control, manifest))
    run_id = uuid.uuid4().hex[:12]
    for operation in args.operations:
        for workers in args.clients:
            for repeat in range(args.repeats):
                phase = f"{args.label}-{operation}-{workers}-{repeat}"
                if args.warmup:
                    warmup = run_case(clients, operation, workers, args.warmup, run_id, phase+"-warmup", topology)
                    save(output / f"warmup-{phase}.json", warmup)
                    results["warmup_errors"] += warmup["errors"]
                    save(output / f"results-{args.label}.json", results)
                    if warmup["errors"]:
                        print(f"Warmup errors retained for {phase}", flush=True)
                before = snapshot_cgroups(topology)
                case = run_case(clients, operation, workers, args.seconds, run_id, phase, topology)
                case["resources_before"] = before
                results["cases"].append(case)
                save(output / f"results-{args.label}.json", results)
                case["resources_after"] = snapshot_cgroups(topology)
                case["lease_loads_after"] = {topology.node_names[i]: control.lease(i)["load"] for i in topology.indices}
                verified = 0
                for cell, item_key in case["write_witnesses"].items():
                    response = clients[topology.owner(int(cell))].get_item(TableName=TABLE, Key=item_key, ConsistentRead=True)
                    if response.get("Item", {}).get("payload", {}).get("S") != "x" * 1024:
                        raise RuntimeError("acknowledged write witness did not read back")
                    verified += 1
                case["write_witnesses_verified"] = verified
                save(output / f"results-{args.label}.json", results)
                compact = {k: case[k] for k in ("operation", "clients", "completed", "errors", "elapsed_s", "requests_per_s", "p50_ms", "p95_ms", "p99_ms", "covers_all_cells")}
                print(json.dumps(compact), flush=True)
    save(output / f"ownership-after-{args.label}.json", verify(control, manifest))
    if results["warmup_errors"] or any(case["errors"] or not case["covers_all_cells"] for case in results["cases"]):
        raise SystemExit("Some cases failed or did not cover every data cell; inspect retained results")


@contextmanager
def telemetry(output, label, topology):
    command = ["docker", "--context", "colima-bdbperf30", "stats", "--format", "{{json .}}",
               *[f"beyonddb-perf30-node{i}" for i in range(1, topology.nodes + 1)], "beyonddb-perf30-store"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    with (output / f"docker-stats-{label}.jsonl").open("w") as log:
        def collect():
            for line in process.stdout:
                line = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", line).strip()
                if not line:
                    continue
                sample = {"observed_at": datetime.now(timezone.utc).isoformat(),
                          "stats": json.loads(line)}
                log.write(json.dumps(sample) + "\n")
                log.flush()
        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        try:
            yield
        finally:
            process.terminate()
            process.wait(timeout=10)
            thread.join(timeout=10)
            process.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "measure", "verify"])
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--operations", nargs="+", choices=["get", "query", "put", "transact_get", "transact_write"],
                        default=["get", "put", "transact_get", "transact_write"])
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8, 30, 60])
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--label", default="main")
    args = parser.parse_args()
    if min(args.clients) < 1 or args.seconds < 1 or args.warmup < 0 or args.repeats < 1:
        parser.error("positive clients, seconds and repeats required; warmup must be nonnegative")
    args.output.mkdir(parents=True, exist_ok=True)
    control = Control(args.run_dir)
    if args.action == "prepare":
        prepare(control, args.output)
    elif args.action == "measure":
        with telemetry(args.output, args.label, control.topology):
            measure(control, args.output, args)
    else:
        print(json.dumps(verify(control, json.loads((args.output / "table.json").read_text())), indent=2))


if __name__ == "__main__":
    main()
