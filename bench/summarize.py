#!/usr/bin/env python3
"""Tables and plots from retained benchmark runs (D-36).

    bench/.venv/bin/python bench/summarize.py bench/results/raw/<experiment>... --out <dir>

Writes runs.csv (one row per run), summary.csv (mean and std per experiment) and PNG plots.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402


def busy_ratio(vertex: dict | None) -> float | None:
    """Busy share of the tasks' whole run, as D-35 defines busy: the rest after idle and backpressured time."""
    if not vertex or not sum(vertex["durationMs"]):
        return None
    return 1 - (sum(vertex["idleMs"]) + sum(vertex["backPressuredMs"])) / sum(vertex["durationMs"])


def run_row(run: Path) -> dict:
    config = json.loads((run / "config.json").read_text())
    effective = config["effective"]
    row = {
        "experiment": config["experiment"], "run": config["runId"], "status": config["status"],
        **{k: effective[k] for k in ("mode", "finish", "partitions", "taskmanagers", "slots", "parallelism")},
        "checkpointing": config["checkpointing"],
    }
    if not (run / "summary.json").exists():
        return row
    s = json.loads((run / "summary.json").read_text())
    vertices = s["job"]["vertices"]
    correctness = s.get("correctness", {})
    row.update({
        "jobState": s["jobState"],
        "replayRecords": s["replay"]["records"],
        "replayDurationMs": s["replay"]["durationMs"],
        "replayRecordsPerSecond": s["replay"]["recordsPerSecond"],
        "jobDurationMs": s["job"]["durationMs"],
        "sourceSpanMs": s["job"]["sourceSpanMs"],
        "jobRecordsPerSecond": s["job"]["recordsPerSecond"],
        "sourceReadSkew": vertices.get("Source: ticks", {}).get("readSkew"),
        "q1ReadSkew": vertices.get("q1-ema", {}).get("readSkew"),
        "sourceBusyRatio": busy_ratio(vertices.get("Source: ticks")),
        "q1BusyRatio": busy_ratio(vertices.get("q1-ema")),
        "emaResults": s["outputs"].get("ema-results"),
        "advisories": s["outputs"].get("advisories"),
        "lateTicks": s["counters"]["lateTicks"] if s["reconciliation"]["countersFinal"]["lateTicks"] else None,
        "reconciliationOk": s["reconciliation"]["ok"],
        "correctness": correctness.get("verdict"),
        "emaDifferences": correctness.get("emaDifferences"),
    })
    return row


def timeseries(run: Path, name: str) -> tuple[list[float], list[float]]:
    """A query's points summed over its series, in seconds from the job's start."""
    metrics = json.loads((run / "metrics.json").read_text())
    start = json.loads((run / "config.json").read_text())["times"]["jobStart"] / 1000
    totals: dict[float, float] = {}
    for series in metrics["queries"][name]["result"]:
        for t, value in series["values"]:
            if value not in ("NaN", "+Inf", "-Inf"):
                totals[t] = totals.get(t, 0.0) + float(value)
    times = sorted(totals)
    return [t - start for t in times], [totals[t] for t in times]


def bars(ax, summary: pl.DataFrame, runs: pl.DataFrame, metric: str, label: str):
    rows = summary.filter(pl.col("metric") == metric).sort("experiment")
    names = rows["experiment"].to_list()
    ax.bar(range(len(names)), rows["mean"].to_list(), yerr=rows["std"].fill_null(0).fill_nan(0).to_list(),
           capsize=4, color="#7a8ca3")
    for i, name in enumerate(names):
        values = runs.filter(pl.col("experiment") == name)[metric].drop_nulls().to_list()
        ax.scatter([i] * len(values), values, color="#1f2a37", s=12, zorder=3)
    ax.set_xticks(range(len(names)), names, rotation=20, ha="right", fontsize=8)
    ax.set_title(label, fontsize=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiments", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, help="default: the experiment directory, when there is one")
    args = parser.parse_args()
    out = args.out or (args.experiments[0] if len(args.experiments) == 1 else None)
    if out is None:
        parser.error("--out is needed with several experiments")
    out.mkdir(parents=True, exist_ok=True)

    run_dirs = {e: sorted(p for p in e.glob("r*") if (p / "config.json").exists()) for e in args.experiments}
    runs = pl.DataFrame([run_row(r) for dirs in run_dirs.values() for r in dirs], infer_schema_length=None)
    runs.write_csv(out / "runs.csv")

    numeric = [c for c, t in runs.schema.items() if t.is_numeric() and c not in ("partitions", "taskmanagers", "slots", "parallelism")]
    summary = (runs.filter(pl.col("status") == "COMPLETE")
               .unpivot(index="experiment", on=numeric, variable_name="metric")
               .drop_nulls("value")
               .group_by("experiment", "metric", maintain_order=True)
               .agg(n=pl.len(), mean=pl.col("value").mean(), std=pl.col("value").std(),
                    min=pl.col("value").min(), max=pl.col("value").max()))
    summary.write_csv(out / "summary.csv")
    print(summary.filter(pl.col("metric").is_in(["jobRecordsPerSecond", "replayRecordsPerSecond", "jobDurationMs"])))

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    bars(axes[0], summary, runs, "jobRecordsPerSecond", "Job: ticks read per second (REST)")
    bars(axes[1], summary, runs, "replayRecordsPerSecond", "Replayer: records per second (report)")
    fig.suptitle("Mean, std and each run", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "throughput.png", dpi=120)

    panels = [("ticksReadPerSubtask", "Ticks read per second"), ("lag", "Lag, records (pendingRecords)")]
    fig, axes = plt.subplots(len(panels), len(run_dirs), figsize=(5 * len(run_dirs), 6), squeeze=False)
    for col, (experiment, dirs) in enumerate(run_dirs.items()):
        for row, (name, label) in enumerate(panels):
            ax = axes[row][col]
            plotted = False
            for r in dirs:
                if (r / "metrics.json").exists():
                    x, y = timeseries(r, name)
                    plotted |= bool(x)
                    ax.plot(x, y, marker=".", label=r.name)
            if not plotted:
                ax.text(0.5, 0.5, "no Prometheus samples during the job", ha="center", va="center",
                        transform=ax.transAxes, fontsize=8)
            ax.set_title(f"{experiment.name}\n{label}" if row == 0 else label, fontsize=9)
            ax.set_xlabel("s from job start", fontsize=8)
            ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "timeseries.png", dpi=120)
    print(f"wrote runs.csv, summary.csv and plots to {out}")


if __name__ == "__main__":
    main()
