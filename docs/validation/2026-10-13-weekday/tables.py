"""Rebuild this directory's tables from the retained runs.

runs.csv has one row per run of bench/configs/weekday/ and one for bounded_defaults.py: the input, the replay
and the watermark settings, the job's counts, the comparer's verdict against the oracle run on the same files
and against the one on the original Monday file, and a SHA-256 of the values the run published. Two runs with
the same digests published the same closes, EMAs and advisories bit for bit. oracles.csv has one row per oracle
run, with the same digests and the bound its input needs.

runs/<run ID>/ keeps each run's config.json, summary.json and comparison reports, oracle/ each oracle run's
manifest and injection/ each rewritten input's. control/ has the first run's export compared with an oracle run
that rejects events, which the gate must refuse.

Run from the repository root:  bench/.venv/bin/python docs/validation/2026-10-13-weekday/tables.py
The comparisons made here, with the original's oracle output and with the control, need Docker.
"""

import csv
import hashlib
import json
import shutil
import subprocess
from array import array
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
RAW = Path("bench/results/raw")
CONFIGS = Path("bench/configs/weekday")
ORACLES = Path("oracle/out")
INJECTED = Path("data/injected")
SCRATCH = Path("correctness/out/weekday")
ORIGINAL = "monday"
# The oracle on the original file at a 0 ms bound, which rejects 79 events.
CONTROL = "monday-bound0"
INPUTS = [ORIGINAL, "monday-disorder-1s", "monday-disorder-5min"]
# Each table's D-18 fields and the key that orders them.
TABLES = {
    "ema": ("ema_results.parquet", ["symbol", "secType", "windowEnd", "close", "ema38", "ema100"], ["symbol", "windowEnd"]),
    "advisory": ("advisories.parquet", ["symbol", "secType", "windowEnd", "type", "ema38", "ema100"],
                 ["symbol", "windowEnd", "type"]),
}


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def digests(directory: Path) -> dict:
    """SHA-256 of each table's D-18 fields in key order, with every double as its eight bytes.

    Equal digests therefore mean equal bits, which neither the gate's tolerance nor a decimal rendering shows.
    """
    out = {}
    for name, (file, fields, key) in TABLES.items():
        rows = pl.read_parquet(directory / file, columns=fields).sort(key)
        digest = hashlib.sha256()
        for field in fields:
            values = rows[field].to_list()
            if rows[field].dtype == pl.Float64:
                digest.update(array("d", values).tobytes())
            elif rows[field].dtype == pl.Int64:
                digest.update(array("q", values).tobytes())
            else:
                digest.update("\n".join(values).encode())
        out[f"{name}Digest"] = digest.hexdigest()
    return out


def at_end_of_input(directory: Path) -> int:
    """Results the job published when its input ended, not when the watermark passed their window.

    The harness waits at least 6 s between the job reading its last record and the drain, so these are the
    results after the last pause of 5 s or more. Without such a pause every result came at the end.
    """
    emitted = pl.read_parquet(directory / "ema_results.parquet", columns=["emitTs"])["emitTs"].sort()
    pauses = (emitted.diff() >= 5000).arg_true()
    return emitted.len() - (pauses[-1] if pauses.len() else 0)


def verdict(report: dict) -> str:
    return "pass" if report["pass"] else "fail"


def differences(report: dict) -> tuple[int, int]:
    return report["ema"]["differences"], sum(report["advisories"]["byKind"].values())


def compare(name: str, oracle: str, export: Path, late_ticks: int | None) -> Path:
    """Runs the comparer on an export against another oracle run than the harness used; returns its report."""
    out = SCRATCH / name
    if not (out / "report.json").exists():
        late = ["--job-late-ticks", str(late_ticks)] if late_ticks is not None else []
        # Exits nonzero when the gate fails; the report says why.
        subprocess.run(["correctness/run.sh", "compare", "--oracle", str(ORACLES / oracle), "--job", str(export), *late,
                        "--out", f"/out/weekday/{name}"], stdout=subprocess.DEVNULL)
    return out / "report.json"


def harness_runs() -> list[Path]:
    names = {p.stem for p in CONFIGS.glob("*.json")}
    return [r for e in RAW.iterdir() if e.name.rsplit("-", 1)[0] in names for r in e.glob("r*")
            if (r / "summary.json").exists()]


def harness_row(run: Path, reference: dict) -> tuple[dict, dict]:
    config, summary, report = load(run / "config.json"), load(run / "summary.json"), load(run / "compare" / "report.json")
    effective, counters, times = config["effective"], summary["counters"], config["times"]
    pacing = summary["replay"].get("pacing") or {}
    args = config["config"].get("replayArgs", [])
    live = effective["mode"] == "live"
    source = Path(config["inputs"][0]["path"]).parent.name
    input_name = ORIGINAL if source == "daily" else source
    late = counters["lateTicks"] if summary["reconciliation"]["countersFinal"]["lateTicks"] else None
    files = {"config.json": run / "config.json", "summary.json": run / "summary.json",
             "compare-report.json": run / "compare" / "report.json"}
    original = report
    if input_name != ORIGINAL:
        files["compare-original-report.json"] = compare(f"{config['runId']}-vs-original", ORIGINAL, run / "export", late)
        original = load(files["compare-original-report.json"])
    values = digests(run / "export")
    row = {
        "run": config["runId"],
        "revision": config["code"]["revision"][:7],
        "status": config["status"],
        "input": input_name,
        "mode": effective["mode"],
        "finish": effective["finish"],
        "rate": int(args[args.index("--rate") + 1]) if "--rate" in args else None,
        "watermarkFlags": "explicit",
        "boundMs": effective["watermarkBoundMs"],
        "idlenessMs": effective["idlenessMs"],
        "parallelism": effective["parallelism"],
        "rows": summary["replay"]["records"],
        "priceEvents": counters["priceEvents"],
        "lateTicks": late,
        "emaResults": summary["outputs"]["ema-results"],
        "advisories": summary["outputs"]["advisories"],
        "reconciled": summary["reconciliation"]["ok"],
        "oracle": Path(report["oracle"]["dir"]).name,
        "oracleLateRejected": report["oracle"]["lateRejected"],
        "emaDifferences": differences(report)[0],
        "advisoryDifferences": differences(report)[1],
        "verdict": summary["correctness"]["verdict"],
        "originalEmaDifferences": differences(original)[0],
        "originalAdvisoryDifferences": differences(original)[1],
        "originalVerdict": verdict(original),
        **values,
        "equalsOracleBitForBit": all(values[k] == reference[k] for k in values),
        "resultsAtEndOfInput": at_end_of_input(run / "export"),
        # How long the job had run when the replay began: it has to stay under the idleness timeout (D-38).
        "replayStartAfterJobS": round((times["replayStart"] - times["jobStart"]) / 1000, 1) if live else None,
        "replayS": round(summary["replay"]["durationMs"] / 1000, 1),
        "achievedRate": pacing.get("achievedRate"),
        "sendLagP50Ms": pacing.get("lagP50Ms"),
        "sendLagP99Ms": pacing.get("lagP99Ms"),
        "sendLagMaxMs": pacing.get("lagMaxMs"),
    }
    return row, files


def defaults_rows(reference: dict, columns: list[str]) -> list[tuple[dict, dict]]:
    """The runs of bounded_defaults.py, which the harness did not make."""
    rows = []
    for run in sorted(SCRATCH.glob("*/record.json")):
        record, report = load(run), load(run.parent / "compare" / "report.json")
        counters, vertices = record["counters"], record["job"]["vertices"]
        values = digests(run.parent / "export")
        revision = record["code"]["revision"][:7]
        row = dict.fromkeys(columns)
        row.update({
            "run": f"{run.parent.name}-{revision}",
            "revision": revision,
            "status": "COMPLETE" if record["job"]["state"] == "FINISHED" and report["pass"] else "INVALID",
            "input": Path(record["oracle"]).name,
            "mode": "preload",
            "finish": "bounded",
            "watermarkFlags": "none",
            "boundMs": 64265001,
            "idlenessMs": 0,
            "parallelism": record["stack"]["parallelism"],
            "rows": vertices["Source: ticks"]["readRecords"],
            "priceEvents": vertices["q1-ema"]["readRecords"],
            "lateTicks": counters["lastPerSubtaskSum"]["lateTicks"] if counters["final"] else None,
            "emaResults": report["ema"]["jobRows"],
            "advisories": report["advisories"]["job"],
            "oracle": Path(report["oracle"]["dir"]).name,
            "oracleLateRejected": report["oracle"]["lateRejected"],
            "emaDifferences": differences(report)[0],
            "advisoryDifferences": differences(report)[1],
            "verdict": verdict(report),
            "originalEmaDifferences": differences(report)[0],
            "originalAdvisoryDifferences": differences(report)[1],
            "originalVerdict": verdict(report),
            **values,
            "equalsOracleBitForBit": all(values[k] == reference[k] for k in values),
            "resultsAtEndOfInput": at_end_of_input(run.parent / "export"),
        })
        files = {"record.json": run, "export-manifest.json": run.parent / "export" / "manifest.json",
                 "compare-report.json": run.parent / "compare" / "report.json"}
        rows.append((row, files))
    return rows


def oracle_rows() -> list[dict]:
    rows = []
    for name in INPUTS:
        manifest = load(ORACLES / name / "manifest.json")
        totals, admission, source = manifest["counts"]["total"], manifest["admission"], manifest["inputs"][0]
        injection = load(INJECTED / name / "injection.json") if name != ORIGINAL else None
        (HERE / "oracle").mkdir(exist_ok=True)
        shutil.copy(ORACLES / name / "manifest.json", HERE / "oracle" / f"{name}.json")
        if injection:
            (HERE / "injection").mkdir(exist_ok=True)
            shutil.copy(INJECTED / name / "injection.json", HERE / "injection" / f"{name}.json")
        rows.append({
            "oracle": name,
            "revision": manifest["git"]["revision"][:7],
            "zoneOffset": manifest["parameters"]["zoneOffset"],
            "inputMd5": source["md5"],
            "disorderMs": injection["options"]["disorderMs"] if injection else 0,
            "seed": injection["options"]["seed"] if injection else None,
            "dataRows": totals["dataRows"],
            "priceEvents": totals["accepted"],
            "lateRejected": totals["excluded"] + totals["lateRejected"],
            # Events whose window an earlier row has already passed, at three bounds, and the bound that admits all.
            "lateAtBound0": admission["lateEventsByBoundMs"]["0"],
            "lateAtBound1s": admission["lateEventsByBoundMs"]["1000"],
            "lateAtBound60s": admission["lateEventsByBoundMs"]["60000"],
            "smallestBoundWithNoLateEventsMs": admission["smallestBoundWithNoLateEventsMs"],
            "emaResults": manifest["outputs"]["ema_results.parquet"]["rows"],
            "advisories": manifest["outputs"]["advisories.parquet"]["rows"],
            **digests(ORACLES / name),
        })
    return rows


def control(run: Path, late_ticks: int | None):
    """Shows the gate failing on this day: one run's export against an oracle run that rejects events."""
    report = compare(f"control-{CONTROL}", CONTROL, run / "export", late_ticks)
    (HERE / "control").mkdir(exist_ok=True)
    shutil.copy(report, HERE / "control" / "compare-report.json")
    shutil.copy(ORACLES / CONTROL / "manifest.json", HERE / "oracle" / f"{CONTROL}.json")
    report = load(report)
    print({"control": CONTROL, "pass": report["pass"], "problems": report["problems"], **report["ema"]["byKind"],
           **report["ema"]["byClass"], "symbols": report["ema"]["symbolsWithDifferences"]})


def order(row: dict) -> tuple:
    return (INPUTS.index(row["input"]), row["mode"] == "live", -row["boundMs"], row["finish"] == "bounded",
            -(row["rate"] or 0), row["run"])


def write(name: str, rows: list[dict]):
    with (HERE / name).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for r in rows:
        print(r)


def main():
    oracles = oracle_rows()
    reference = {k: v for k, v in oracles[0].items() if k.endswith("Digest")}
    runs = [harness_row(run, reference) for run in harness_runs()]
    runs += defaults_rows(reference, list(runs[0][0]))
    runs.sort(key=lambda pair: order(pair[0]))
    for row, files in runs:
        target = HERE / "runs" / row["run"]
        target.mkdir(parents=True, exist_ok=True)
        for name, source in files.items():
            shutil.copy(source, target / name)
    write("oracles.csv", oracles)
    write("runs.csv", [row for row, _ in runs])
    first, files = runs[0]
    control(files["config.json"].parent, first["lateTicks"])


if __name__ == "__main__":
    main()
