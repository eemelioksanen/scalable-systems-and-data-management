"""Rebuild this directory's table from the retained runs in bench/results/raw/.

runs.csv has one row per run of bench/configs/watermark/: its code revision, bound, idleness and replay, the
job's late count against the oracle's, the correctness verdict, and for a live run the time from a window's
last price event entering Kafka to its result (emitTs - maxIngestTs). runs/<name>-<revision>-r<k>/ keeps each
run's config.json and summary.json.

Run from the repository root:  bench/.venv/bin/python docs/validation/2026-10-13-watermark/tables.py
"""

import csv
import json
import shutil
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
RAW = Path("bench/results/raw")
CONFIGS = Path("bench/configs/watermark")
ORDER = ["week-preload", "monwed-live", "subset12-live"]


def runs() -> list[Path]:
    names = {p.stem for p in CONFIGS.glob("*.json")}
    found = [r for e in RAW.iterdir() if e.name.rsplit("-", 1)[0] in names for r in e.glob("r*")
             if (r / "summary.json").exists()]
    return sorted(found, key=lambda r: (next(i for i, p in enumerate(ORDER) if r.parent.name.startswith(p)),
                                        r.parent.name.rsplit("-", 1)[1], r.name))


def missing_windows(run: Path) -> int:
    """Windows the oracle has and the job never published."""
    differences = pl.read_parquet(run / "compare" / "ema_differences.parquet", columns=["kind"])
    return differences.filter(pl.col("kind") == "oracleOnly").height


def latency(run: Path, replay_end_ms: int) -> dict:
    """Last ingest to emit, over the results the job published before the replay ended.

    The rest were still open when the replay ended: the last events or the drain fired them.
    """
    results = pl.read_parquet(run / "export" / "ema_results.parquet", columns=["maxIngestTs", "emitTs"])
    during = results.filter(pl.col("emitTs") <= replay_end_ms)
    wait = (during["emitTs"] - during["maxIngestTs"]) / 1000
    row = {"emittedAfterReplay": results.height - during.height}
    for name, q in (("P50", 0.5), ("P95", 0.95), ("P99", 0.99)):
        row[f"ingestToEmit{name}S"] = round(wait.quantile(q, interpolation="nearest"), 2) if during.height else None
    row["ingestToEmitMaxS"] = round(wait.max(), 2) if during.height else None
    return row


def row(run: Path) -> dict:
    config = json.loads((run / "config.json").read_text())
    summary = json.loads((run / "summary.json").read_text())
    report = json.loads((run / "compare" / "report.json").read_text())
    effective, correctness, counters = config["effective"], summary["correctness"], summary["counters"]
    pacing = summary["replay"].get("pacing") or {}
    args = config["config"].get("replayArgs", [])
    live = effective["mode"] == "live"
    revision = config["code"]["revision"][:7]
    out = {
        "run": f"{run.parent.name.rsplit('-', 1)[0]}-{revision}-{run.name}",
        "revision": revision,
        "status": config["status"],
        "mode": effective["mode"],
        "rate": args[args.index("--rate") + 1] if "--rate" in args else None,
        "boundMs": effective["watermarkBoundMs"],
        "idlenessMs": effective["idlenessMs"],
        "parallelism": effective["parallelism"],
        "priceEvents": counters["priceEvents"],
        "lateTicks": counters["lateTicks"] if summary["reconciliation"]["countersFinal"]["lateTicks"] else None,
        "oracleBoundMs": report["oracle"]["watermarkBoundMs"],
        "oracleLateRejected": report["oracle"]["lateRejected"],
        "emaResults": summary["outputs"]["ema-results"],
        "advisories": summary["outputs"]["advisories"],
        "emaDifferences": correctness["emaDifferences"],
        "missingWindows": missing_windows(run),
        "advisoryDifferences": correctness["advisoryDifferences"],
        "verdict": correctness["verdict"],
        "openWindowsMax": int(sum(s["max"] or 0 for s in summary["metrics"]["openWindows"])),
        "achievedRate": pacing.get("achievedRate"),
        "sendLagP50Ms": pacing.get("lagP50Ms"),
        "sendLagP99Ms": pacing.get("lagP99Ms"),
        "sendLagMaxMs": pacing.get("lagMaxMs"),
    }
    blank = {"emittedAfterReplay": None, "ingestToEmitP50S": None, "ingestToEmitP95S": None, "ingestToEmitP99S": None,
             "ingestToEmitMaxS": None}
    out.update(latency(run, config["times"]["replayEnd"]) if live else blank)
    return out


def main():
    rows = []
    for run in runs():
        rows.append(row(run))
        target = HERE / "runs" / rows[-1]["run"]
        target.mkdir(parents=True, exist_ok=True)
        for file in ("config.json", "summary.json"):
            shutil.copy(run / file, target / file)
    with (HERE / "runs.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for r in rows:
        print(r)


if __name__ == "__main__":
    main()
