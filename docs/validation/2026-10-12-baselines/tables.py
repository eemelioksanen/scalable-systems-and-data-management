"""Rebuild this directory's tables from the retained runs in bench/results/raw/.

runs.csv comes from summarize.py; subtask-load.csv compares each preload's subtask records with predict_load.py;
preload-phase.csv summarizes the replayer and the broker while each preload ran, from metrics.json; runs/<name>/ keeps
each run's config.json and summary.json.

Run from the repository root:  bench/.venv/bin/python docs/validation/2026-10-12-baselines/tables.py
"""

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = Path("bench/results/raw")
RUNS = sorted(p for p in RAW.glob("*-2026*") if p.name.startswith(("week-", "day-")))


def name(experiment: Path) -> str:
    return experiment.name.split("-2026")[0]


def predicted(partitions: int, parallelism: int, exchanges: str | None) -> dict:
    command = [sys.executable, "bench/predict_load.py", "--partitions", str(partitions), "--parallelism", str(parallelism)]
    if exchanges:
        command += ["--exchanges", exchanges]
    out = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    return next(csv.DictReader(out.splitlines()))


def subtask_load() -> list[dict]:
    rows = []
    for experiment in RUNS:
        if not name(experiment).startswith("week-"):
            continue
        config = json.loads((experiment / "r1" / "config.json").read_text())
        vertices = json.loads((experiment / "r1" / "summary.json").read_text())["job"]["vertices"]
        args = config["config"].get("replayArgs", [])
        exchanges = args[args.index("--exchanges") + 1] if "--exchanges" in args else None
        effective = config["effective"]
        prediction = predicted(effective["partitions"], effective["parallelism"], exchanges)
        source = "/".join(map(str, vertices["Source: ticks"]["readRecords"]))
        q1 = "/".join(map(str, vertices["q1-ema"]["readRecords"]))
        rows.append({"run": name(experiment), "partitions": effective["partitions"],
                     "parallelism": effective["parallelism"], "exchanges": exchanges or "all",
                     "sourceRecords": source, "sourceSkew": vertices["Source: ticks"]["readSkew"],
                     "q1PriceEvents": q1, "q1Skew": vertices["q1-ema"]["readSkew"],
                     "matchesPrediction": source == prediction["sourceRecords"] and q1 == prediction["q1PriceEvents"]})
    return rows


def preload_phase() -> list[dict]:
    """From 20 s into each preload to its end, so the 15 s rate windows hold no reset traffic."""
    queries = {"replayCores": ("cpuCores", "-replay"), "kafkaCores": ("cpuCores", "-kafka-1"),
               "hostCores": ("hostCpuCores", ""), "brokerRequestHandlerIdle": ("brokerRequestHandlerIdle", ""),
               "ticksIntoKafka": ("ticksIntoKafka", "")}
    rows = []
    for experiment in RUNS:
        if not name(experiment).startswith("week-"):
            continue
        times = json.loads((experiment / "r1" / "config.json").read_text())["times"]
        metrics = json.loads((experiment / "r1" / "metrics.json").read_text())["queries"]
        start, end = times["replayStart"] / 1000 + 20, times["replayEnd"] / 1000
        row = {"run": name(experiment)}
        for key, (query, container) in queries.items():
            per_time: dict[float, float] = {}
            for series in metrics[query]["result"]:
                if series["metric"].get("name", "").endswith(container) or not container:
                    for t, v in series["values"]:
                        if start <= float(t) <= end and v != "NaN":
                            per_time[float(t)] = per_time.get(float(t), 0) + float(v)
            values = list(per_time.values())
            row[key + "Mean"] = round(sum(values) / len(values), 3) if values else None
            row[key + "Max"] = round(max(values), 3) if values else None
        rows.append(row)
    return rows


def write(path: Path, rows: list[dict]):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    for experiment in RUNS:
        target = HERE / "runs" / name(experiment)
        target.mkdir(parents=True, exist_ok=True)
        for file in ("config.json", "summary.json"):
            shutil.copy(experiment / "r1" / file, target / file)
    subprocess.run([sys.executable, "bench/summarize.py", *map(str, RUNS), "--out", str(RAW / "baselines")],
                   check=True, capture_output=True)
    shutil.copy(RAW / "baselines" / "runs.csv", HERE / "runs.csv")
    shutil.copy(RAW / "baselines" / "throughput.png", HERE / "throughput.png")
    live = [str(p) for p in RUNS if name(p).startswith("day-")]
    subprocess.run([sys.executable, "bench/summarize.py", *live, "--out", str(RAW / "baselines-live")],
                   check=True, capture_output=True)
    shutil.copy(RAW / "baselines-live" / "timeseries.png", HERE / "timeseries-live.png")
    write(HERE / "subtask-load.csv", subtask_load())
    write(HERE / "preload-phase.csv", preload_phase())
    with (HERE / "predicted-load.csv").open("w") as f:
        subprocess.run([sys.executable, "bench/predict_load.py"], stdout=f, check=True)


if __name__ == "__main__":
    main()
