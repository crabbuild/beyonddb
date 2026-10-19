#!/usr/bin/env python3
"""Summarize retained benchmark windows without dropping failed cases."""
import argparse
import csv
import json
from pathlib import Path

from topology import Topology, covers_all

LABELS = {"get": "GetItem", "query": "Query", "put": "PutItem",
          "transact_get": "TransactGetItems", "transact_write": "TransactWriteItems"}
COLUMNS = ["operation", "clients", "completed", "errors", "elapsed_s", "requests_per_s",
           "items_per_s", "p50_ms", "p95_ms", "p99_ms", "covers_all_cells"]


def field(text, name):
    for line in text.splitlines():
        if line.startswith(name + " "):
            return int(line.split()[1])
    raise ValueError(f"missing resource counter {name}")


def read_optional(path):
    return json.loads(path.read_text()) if path.exists() else None


def render(result, source, summary_clients=8):
    topology = Topology.from_result(result)
    cases = result["cases"]
    metadata = read_optional(source / "meta.json") or {}
    revision = metadata.get("source_revision", "unrecorded")
    placement = result.get("transaction_placement", "different cells and nodes")
    failed = sum(bool(c["errors"]) for c in cases)
    lines = ["# Local BeyondDB throughput and latency", "",
             f"**{len(cases)} measurement windows completed; {failed} contained request errors.** "
             f"Warmup errors: {result['warmup_errors']}. Every window retains its coverage and errors below.", "",
             f"{topology.nodes} nodes, each limited to 1 vCPU, 3 GiB RAM and a 5 GiB data filesystem. "
             f"One table spans {topology.cells} data cells, {topology.cells_per_node} per node. "
             "Metadata cells are additional; authoritative ownership observations are retained separately. "
             f"Release server `{revision[:7]}`; Cellar `=0.1.0`; unchanged ExtendDB pin.", "",
             "Uniform 1 KiB payloads; signed SDK calls; retries disabled; strong point/range reads. "
             f"Two-item transactions use {placement}. Each window has "
             f"{result.get('warmup_s', 5)} seconds of separate warmup. "
             "Writes wait for local RustFS bucket publication. Native balancing is paused for controlled "
             "placement. The client and the 1 CPU / 4 GiB object store are outside the node budgets.", "",
             f"## At {summary_clients} clients", "",
             "| Operation | Requests/s | p50 ms | p95 ms | p99 ms | Samples | Errors |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    summary = [c for c in cases if c["clients"] == summary_clients]
    for operation in LABELS:
        for c in summary:
            if c["operation"] == operation:
                lines.append("| " + " | ".join(str(v) for v in [LABELS[operation], c["requests_per_s"],
                    c["p50_ms"], c["p95_ms"], c["p99_ms"], c["completed"], c["errors"]]) + " |")
    if not summary:
        lines += ["", "No measurement windows at this client count."]
    lines += ["", "![Throughput and p99 latency](throughput-latency.png)", ""]
    label = result.get("label", "main")
    ownership = read_optional(source / f"ownership-after-{label}.json")
    resources = read_optional(source / "resource-summary.json")
    proof = read_optional(source / "runtime-proof.json")
    if ownership:
        owned = sum(len(n["cells"]) for n in ownership["nodes"])
        lines += ["## Ownership observations", "",
                  f"Final table placement: `{ownership['data_cells_per_node']}`. "
                  f"The complete inventory has {owned:,} owned cells, including metadata/coordinators.", ""]
        if resources:
            lines += ["| Node | Data cells | All owned cells | Peak sampled memory GiB | Maximum observed disk use GiB |",
                      "| --- | ---: | ---: | ---: | ---: |"]
            for node in ownership["nodes"]:
                r = resources.get(f"beyonddb-{node['node']}")
                if r:
                    lines.append(f"| {node['node']} | {ownership['data_cells_per_node'].get(node['node'], 0)} | "
                                 f"{len(node['cells'])} | {r['max_sampled_memory_bytes']/1073741824:.2f} | "
                                 f"{r['max_observed_disk_used_bytes']/1073741824:.2f} |")
            lines += [""]
    if proof:
        refusals = sum(p.get("overload_by_class", {}).get("beyonddb-account", 0) for p in proof.values())
        lines += [f"Logs contain {refusals:,} shared-account admission refusals. "
                  "These internal events, including paced reads, differ from public request errors.", ""]
    stop = read_optional(source / "stop-proof.json")
    if stop:
        clean = all(not p["Running"] and not p["OOMKilled"] and p["ExitCode"] == 0 for p in stop.values())
        lines += [f"Retained shutdown observations: {len(stop)} processes; clean exits without OOM: {clean}.", ""]
    witnesses = sum(c.get("write_witnesses_verified", 0) for c in cases)
    lines += [f"Acknowledged-write witnesses read back consistently: **{witnesses}**. "
              "These sample acknowledged values; failed mutations retain uncertain outcomes.", "",
              "## All measurement windows", "",
              f"| Operation | Clients | Requests/s | Items/s | Completions | Errors | Elapsed s | p50 ms | p95 ms | p99 ms | All {topology.cells} cells |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |"]
    rows = []
    for c in cases:
        columns = [LABELS[c["operation"]], c["clients"], c["requests_per_s"], c["items_per_s"],
                   c["completed"], c["errors"], c["elapsed_s"], c["p50_ms"], c["p95_ms"], c["p99_ms"],
                   "yes" if covers_all(c) else "NO"]
        lines.append("| " + " | ".join(str(v) for v in columns) + " |")
        rows.append({**{k: c[k] for k in COLUMNS if k != "covers_all_cells"}, "covers_all_cells": covers_all(c)})
    lines += ["", f"Measured completions: **{sum(c['completed'] for c in cases):,}**. "
              f"Measured errors: **{sum(c['errors'] for c in cases)}**. "
              f"Warmup errors: **{result['warmup_errors']}**.", "",
              "## Resource observations", "",
              "CPU use is cgroup CPU time divided by each case's actual elapsed time. "
              "Throttling is the percentage of scheduling periods throttled, not lost wall time.", "",
              "| Operation | Clients | Node | CPU % | Throttled periods % | RSS MiB after | Owned cells after | Open FDs after |",
              "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for c in cases:
        for i in topology.indices:
            if "resources_after" not in c:
                continue
            before, after = c["resources_before"][f"node{i+1}"], c["resources_after"][f"node{i+1}"]
            cpu = (field(after, "usage_usec") - field(before, "usage_usec")) / (c["elapsed_s"]*10000)
            periods = field(after, "nr_periods") - field(before, "nr_periods")
            throttled = field(after, "nr_throttled") - field(before, "nr_throttled")
            load = c["lease_loads_after"][topology.node_names[i]]
            lines.append(f"| {LABELS[c['operation']]} | {c['clients']} | {i+1} | {cpu:.1f} | "
                         f"{100*throttled/max(periods,1):.1f} | {load['rss_bytes']/1048576:.1f} | "
                         f"{load['owned_cells']} | {load['open_fds']} |")
    order = list(dict.fromkeys(c["operation"] for c in cases))
    lines += ["", "## Interpretation and limits", "",
              "All nodes share one local VM and physical storage. This is not independent-machine "
              "capacity qualification. The closed-loop sweep includes signing, authentication, routing "
              "and durability. It does not establish an arrival-rate SLO or automatic-balancing performance. "
              "Warmup is excluded. Workloads share evolving table/coordinator state; observed operation order: "
              + ", ".join(LABELS[op] for op in order) + ". "
              "Workers stop after three errors, so failed windows are not steady capacity measurements. "
              "SDK retries are disabled; failed mutation outcomes remain uncertain. "
              "Review retained resource counters and logs for contention or interruptions.", ""]
    low = [f"{LABELS[c['operation']]} / {c['clients']} clients ({c['completed']} samples)"
           for c in cases if c["tail_sample_warning"]]
    if low:
        lines += ["p99 estimates have fewer than 1,000 successful samples in: " + "; ".join(low) + ".", ""]
    planned = result.get("planned_cases", 20)
    if len(cases) != planned:
        lines += [f"**Incomplete matrix: {len(cases)} of {planned} planned cases.**", ""]
    return "\n".join(lines), rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary-clients", type=int, default=8)
    args = parser.parse_args()
    report, rows = render(json.loads(args.input.read_text()), args.input.parent, args.summary_clients)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "REPORT.md").write_text(report)
    with (args.output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} cases, including every failed or incomplete case.")


if __name__ == "__main__":
    main()
