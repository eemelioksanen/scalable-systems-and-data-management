"""Summarize the variant check (run.sh) into docs/validation/2026-11-17-variants/runs.json.

    uv run --with polars==1.44.2 python scripts/validation/issue23/report.py

For each config, the newest experiment's verdicts, and a digest of its published records. The
variants D-42 says publish the same output as the baseline must have the baseline's digests.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
CONFIGS = ROOT / "scripts/validation/issue23/configs"
RAW = ROOT / "bench/results/raw"
OUT = ROOT / "docs/validation/2026-11-17-variants/runs.json"
EMA = ["symbol", "secType", "windowEnd", "close", "ema38", "ema100"]
ADVISORY = ["symbol", "secType", "windowEnd", "type", "ema38", "ema100"]
SAME_AS_BASELINE = ["issue23-window", "issue23-rocksdb", "issue23-binary", "issue23-early-filter"]


def digest(path: Path, columns: list[str]) -> str:
    rows = pl.read_parquet(path).select(columns).sort(["symbol", "windowEnd"])
    return hashlib.sha256(rows.write_csv().encode()).hexdigest()


def backend(log: Path) -> str | None:
    text = log.read_text() if log.exists() else ""
    if re.search(r"RocksDB", text):
        return "rocksdb"
    if re.search(r"HashMapStateBackend", text):
        return "hashmap"
    return None


def run(name: str) -> dict:
    experiment = sorted(RAW.glob(f"{name}-2*"))[-1]
    r1 = experiment / "r1"
    config = json.loads((r1 / "config.json").read_text())
    summary = json.loads((r1 / "summary.json").read_text())
    replay = json.loads((r1 / "replay.json").read_text())
    return {
        "experiment": experiment.name,
        "status": config["status"],
        "revision": config["code"]["revision"],
        "dirty": config["code"]["dirty"],
        "requested": config["effective"]["variant"],
        "jobEffective": config["job"]["effective"],
        "replay": {"encoding": replay.get("encoding"), "earlyFilter": replay["selection"].get("earlyFilter"),
                   "attempted": replay["counts"]["attempted"], "filtered": replay["counts"].get("filtered")},
        "taskmanagerBackendLog": backend(experiment / "taskmanager.log"),
        "outputs": summary["outputs"],
        "lateTicks": summary["counters"]["lateTicks"],
        "reconciliation": summary["reconciliation"]["ok"],
        "failedChecks": [c["check"] for c in summary["reconciliation"]["checks"] if not c["ok"]],
        "correctness": summary["correctness"]["verdict"],
        "emaDifferences": summary["correctness"]["emaDifferences"],
        "advisoryDifferences": summary["correctness"]["advisoryDifferences"],
        "emaDigest": digest(r1 / "export/ema_results.parquet", EMA),
        "advisoryDigest": digest(r1 / "export/advisories.parquet", ADVISORY),
    }


def main() -> int:
    names = [json.loads(p.read_text())["name"] for p in sorted(CONFIGS.glob("*.json"))]
    runs = {name: run(name) for name in names}
    base = runs["issue23-baseline"]
    checks = {
        "everyRunComplete": all(r["status"] == "COMPLETE" for r in runs.values()),
        "everyCorrectnessPass": all(r["correctness"] == "pass" for r in runs.values()),
        "jobRanTheRequestedVariant": all(
            all(r["jobEffective"][k] == v for k, v in r["requested"].items() if k != "filtering")
            for r in runs.values()),
        "sameOutputAsBaseline": {n: runs[n]["emaDigest"] == base["emaDigest"]
                                 and runs[n]["advisoryDigest"] == base["advisoryDigest"] for n in SAME_AS_BASELINE},
        "carryForwardDiffersFromBaseline": runs["issue23-carry-forward"]["emaDigest"] != base["emaDigest"],
        "allVariantsEqualsCarryForward": runs["issue23-all"]["emaDigest"] == runs["issue23-carry-forward"]["emaDigest"]
                                         and runs["issue23-all"]["advisoryDigest"]
                                         == runs["issue23-carry-forward"]["advisoryDigest"],
    }
    OUT.write_text(json.dumps({"checks": checks, "runs": runs}, indent=2) + "\n")
    print(json.dumps(checks, indent=2))
    flat = [checks["everyRunComplete"], checks["everyCorrectnessPass"], checks["jobRanTheRequestedVariant"],
            *checks["sameOutputAsBaseline"].values(), checks["carryForwardDiffersFromBaseline"],
            checks["allVariantsEqualsCarryForward"]]
    return 0 if all(flat) else 1


if __name__ == "__main__":
    sys.exit(main())
