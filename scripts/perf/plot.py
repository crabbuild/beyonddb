#!/usr/bin/env python3
"""Render the measured concurrency sweep with matplotlib (optional)."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from report import LABELS
from topology import Topology, covers_all
from matplotlib.lines import Line2D


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = json.loads(args.input.read_text())
    topology = Topology.from_result(result)
    cases = result["cases"]
    colors = ["#0072b2", "#009e73", "#e69f00", "#cc79a7", "#d55e00"]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    for (operation, label), color in zip(LABELS.items(), colors):
        rows = sorted([c for c in cases if c["operation"] == operation], key=lambda c: c["clients"])
        if not rows:
            continue
        for ax, metric in zip(axes, ("requests_per_s", "p99_ms")):
            good = [c for c in rows if c[metric] is not None and c[metric] > 0]
            ax.plot([c["clients"] for c in good], [c[metric] for c in good],
                    marker="o", color=color, linewidth=2, label=label)
            bad = [c for c in good if c["errors"] or not covers_all(c)]
            if bad:
                ax.scatter([c["clients"] for c in bad], [c[metric] for c in bad],
                           color="red", marker="x", zorder=5, s=90)
    for ax in axes:
        ax.set_xlabel("Configured concurrent clients")
        ax.set_xticks(sorted({c["clients"] for c in cases}))
        ax.set_yscale("log")
        ax.grid(True, which="major", alpha=0.22)
    axes[0].set_title("Successful API throughput")
    axes[0].set_ylabel("Requests / second (log scale)")
    axes[1].set_title("p99 response latency")
    axes[1].set_ylabel("Milliseconds (log scale)")
    fig.suptitle(f"BeyondDB · {topology.nodes} × 1 vCPU / 3 GiB / 5 GiB data · {topology.cells} data cells / one table", fontsize=13, fontweight="bold")
    handles, labels = axes[0].get_legend_handles_labels()
    if any(c["errors"] or not covers_all(c) for c in cases):
        handles.append(Line2D([], [], color="red", marker="x", linestyle="none"))
        labels.append("Errors or incomplete coverage")
    fig.legend(handles, labels, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 0.06), frameon=False)
    seconds = ",".join(str(s) for s in sorted({c["requested_s"] for c in cases}))
    fig.text(0.5, 0.02, f"1 KiB payload · signed SDK · {seconds} s closed-loop windows · bucket durability · local shared VM", ha="center", color="#555555", fontsize=9)
    fig.subplots_adjust(top=0.82, bottom=0.25, left=0.07, right=0.98, wspace=0.26)
    args.output.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output / "throughput-latency.png", dpi=180, facecolor="white")
    svg = args.output / "throughput-latency.svg"
    fig.savefig(svg, facecolor="white")
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    plt.close(fig)


if __name__ == "__main__":
    main()
