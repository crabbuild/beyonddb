#!/usr/bin/env python3
"""Summarize retained benchmark windows without dropping failed cases."""
import argparse
import csv
import json
from pathlib import Path

LABELS = {"get": "GetItem", "query": "Query", "put": "PutItem",
          "transact_get": "TransactGetItems", "transact_write": "TransactWriteItems"}


def field(text, name):
    for line in text.splitlines():
        if line.startswith(name + " "):
            return int(line.split()[1])
    raise ValueError(f"missing resource counter {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = json.loads(args.input.read_text())
    cases = result["cases"]
    meta_path = args.input.parent / "meta.json"
    metadata = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    revision = metadata.get("source_revision", "unrecorded")
    failed = [c for c in cases if c["errors"]]
    outcome = (f"**{len(cases)} measurement windows completed; {len(failed)} contained request errors.** "
               f"Warmup errors: {result["warmup_errors"]}. Every window retains its actual coverage and errors below.")
    lines = ["# Local BeyondDB throughput and latency", "", outcome, "",
             "Three nodes, each limited to 1 vCPU, 3 GiB RAM and a 5 GiB data filesystem. "
             "One table spans 30 verified data cells, ten per node. Metadata cells are additional. "
             f"Release server `{revision[:7]}`; Cellar `=0.1.0`; unchanged ExtendDB pin.", "",
             "Uniform 1 KiB payloads; signed SDK calls; retries disabled; strong point/range reads; "
             "two-item transactions cross nodes. Each window has five seconds of separate warmup. "
             "Writes wait for local RustFS bucket publication. Native balancing is paused to retain "
             "controlled placement. The client and object store are outside the node budgets.", "",
             "## At eight clients", "",
             "| Operation | Requests/s | p50 ms | p95 ms | p99 ms | Samples | Errors |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for operation in LABELS:
        for c in cases:
            if c["operation"] == operation and c["clients"] == 8:
                lines.append("| " + " | ".join(str(v) for v in [LABELS[operation], c["requests_per_s"],
                    c["p50_ms"], c["p95_ms"], c["p99_ms"], c["completed"], c["errors"]]) + " |")
    lines += ["", "Transactions contain two items on different nodes. Transaction p99 estimates "
              "have fewer than 1,000 successful samples at this concurrency.", "",
              "![Throughput and p99 latency](throughput-latency.png)", ""]
    proof_path = args.input.parent / "runtime-proof.json"
    ownership_path = args.input.parent / "ownership-after-main.json"
    resources_path = args.input.parent / "resource-summary.json"
    if proof_path.exists() and ownership_path.exists() and resources_path.exists():
        proof = json.loads(proof_path.read_text())
        ownership = json.loads(ownership_path.read_text())
        resources = json.loads(resources_path.read_text())
        cells = sum(len(n["cells"]) for n in ownership["nodes"])
        refusals = sum(p.get("overload_by_class", {}).get("beyonddb-account", 0) for p in proof.values())
        lines += ["## Findings", "",
                  f"- Final ownership is ten table data cells per node. The complete inventory has {cells:,} "
                  f"owned cells: {cells-30:,} system/coordinator cells in addition to the 30 data cells.",
                  f"- Logs contain {refusals:,} admission refusals on the shared account cell at its native "
                  "16-request limit. These are internal events, including paced reads; their count is not "
                  "the public API error count. Both transaction APIs returned public 503s at 30 and 60 clients.",
                  "- All three database nodes and the object store remained free of OOM kills in this final run. "
                  "All four processes subsequently stopped cleanly with exit status zero.",
                  "- Each write window read back one acknowledged witness per range: 240 consistent read-backs "
                  "in total. This samples acknowledged values; failed mutation outcomes are not classified as rollbacks.", "",
                  "| Node | Data cells | All owned cells | Peak sampled cgroup memory GiB | Maximum observed disk use GiB |",
                  "| --- | ---: | ---: | ---: | ---: |"]
        for i, node in enumerate(ownership["nodes"], 1):
            r = resources[f"beyonddb-perf30-node{i}"]
            lines.append(f"| {i} | 10 | {len(node['cells'])} | {r['max_sampled_memory_bytes']/1073741824:.2f} | "
                         f"{r['max_observed_disk_used_bytes']/1073741824:.2f} |")
        store = resources["beyonddb-perf30-store"]["max_sampled_memory_bytes"]/1073741824
        lines += ["", f"The separate object store used at most {store:.2f} GiB in sampled Docker stats, "
                  "within its 4 GiB limit. Disk usage includes retained earlier trials on the same bounded filesystems. "
                  "The 5 GiB images provide about 4.84 GiB after ext4 metadata.", ""]
    lines += ["## All measurement windows", "",
             "| Operation | Clients | Requests/s | Items/s | Completions | Errors | Elapsed s | p50 ms | p95 ms | p99 ms | All 30 cells |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |"]
    rows = []
    for c in cases:
        columns = [LABELS[c["operation"]], c["clients"], c["requests_per_s"], c["items_per_s"],
                   c["completed"], c["errors"], c["elapsed_s"], c["p50_ms"], c["p95_ms"], c["p99_ms"],
                   "yes" if c["covers_all_30_cells"] else "NO"]
        lines.append("| " + " | ".join(str(v) for v in columns) + " |")
        rows.append({k: c[k] for k in ["operation", "clients", "completed", "errors", "elapsed_s",
                    "requests_per_s", "items_per_s", "p50_ms", "p95_ms", "p99_ms", "covers_all_30_cells"]})
    lines += ["", f"Measured completions: **{sum(c['completed'] for c in cases):,}**. "
              f"Measured errors: **{sum(c['errors'] for c in cases)}**. "
              f"Warmup errors: **{result['warmup_errors']}**.", "",
              "## Resource observations", "",
              "CPU use is cgroup CPU time divided by each case's actual elapsed time. "
              "Throttling is the percentage of scheduling periods that were throttled, not lost wall time.", "",
              "| Operation | Clients | Node | CPU % | Throttled periods % | RSS MiB after | Owned cells after | Open FDs after |",
              "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for c in cases:
        for i in range(1, 4):
            if "resources_after" not in c:
                continue
            before, after = c["resources_before"][f"node{i}"], c["resources_after"][f"node{i}"]
            cpu = (field(after, "usage_usec") - field(before, "usage_usec")) / (c["elapsed_s"]*10000)
            periods = field(after, "nr_periods") - field(before, "nr_periods")
            throttled = field(after, "nr_throttled") - field(before, "nr_throttled")
            load = c["lease_loads_after"][f"perf30-node{i}"]
            lines.append(f"| {LABELS[c['operation']]} | {c['clients']} | {i} | {cpu:.1f} | "
                         f"{100*throttled/max(periods,1):.1f} | {load['rss_bytes']/1048576:.1f} | "
                         f"{load['owned_cells']} | {load['open_fds']} |")
    lines += ["", "## Interpretation and limits", "",
              "The initial trial failed at Docker's 1,024-file soft limit. The next trial retained "
              "transaction-write errors when the default 128-cell residency budget was reached. "
              "The measured configuration uses `nofile=65536:65536` and `max_active_cells=2048`; "
              "The local store was subsequently OOM-killed at its separate 1 GiB limit, interrupting the 60-second matrix after 13 windows. The final store has 4 GiB; database node CPU, RAM and disk allocations were unchanged. Earlier trials and their errors are retained.", "",
              "All nodes share one local VM and physical storage. The macOS host was already under "
              "substantial memory pressure (about 30 GiB swap used before the first smoke test); this is not an "
              "independent-machine capacity result. The closed-loop sweep includes client signing, "
              "authentication, routing and durability. It does not establish an open-loop arrival-rate "
              "SLO or automatic-balancing performance. Warmup is excluded from the table. "
              "Operations share the evolving table and coordinator state; workload order is "
              "transaction writes, transaction reads, puts, gets, then range queries. Workers stop "
              "after three errors, so failed windows are not steady capacity measurements. Failed "
              "mutation outcomes remain uncertain; no SDK retries or rollback assumptions are used.", ""]
    low = [f"{LABELS[c['operation']]} / {c['clients']} clients ({c['completed']} samples)"
           for c in cases if c["tail_sample_warning"]]
    if low:
        lines += ["p99 estimates have fewer than 1,000 successful samples in: " + "; ".join(low) + ".", ""]
    if len(cases) != 20:
        lines += [f"**Incomplete matrix: {len(cases)} of 20 planned cases.**", ""]
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "REPORT.md").write_text("\n".join(lines))
    with (args.output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(cases)} cases, including every failed or incomplete case.")


if __name__ == "__main__":
    main()
