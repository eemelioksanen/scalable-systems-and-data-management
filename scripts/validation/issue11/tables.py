"""Builds the #11 evidence in docs/validation/2026-11-18-issue11/ from the retained runs in bench/results/raw/.

For every #11 config it takes the latest experiment directory, then:
- runs bench/summarize.py per group (scale, exchanges, traders, variants, live, calibration) into <out>/<group>/;
- copies each run's config.json, summary.json and comparison reports into <out>/runs/<experiment>/<rK>/;
- writes tables.md (mean ± sample std over COMPLETE repeats, n, failures) and resources.png (TaskManager, Kafka and
  replayer CPU over each scale run).

    MPLCONFIGDIR=$TMPDIR bench/.venv/bin/python scripts/validation/issue11/tables.py
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / "bench/results/raw"
CONFIGS = ROOT / "bench/configs/issue11"
OUT = ROOT / "docs/validation/2026-11-18-issue11"

GROUPS = {
    "scale": lambda n: n.startswith("scale-"),
    "exchanges": lambda n: n.startswith("lane-exch-") or n == "lane-base-p4",
    "traders": lambda n: n.startswith("lane-traders-") or n == "lane-base-p4",
    "variants": lambda n: n.startswith("lane-variant-") or n == "lane-base-p4",
    "live": lambda n: n.startswith("lane-live-"),
    "calibration": lambda n: n.startswith("calib-"),
}


def latest(name: str) -> Path | None:
    dirs = sorted(d for d in RAW.glob(f"{name}-20[0-9][0-9][0-9][0-9][0-9][0-9]T*") if d.is_dir())
    return dirs[-1] if dirs else None


def experiments() -> dict[str, Path]:
    names = sorted(p.stem for p in CONFIGS.glob("*/*.json"))
    return {n: d for n in names if (d := latest(n))}


def retain(exps: dict[str, Path]) -> None:
    for name, exp in exps.items():
        for run in sorted(exp.glob("r*")):
            target = OUT / "runs" / name / run.name
            target.mkdir(parents=True, exist_ok=True)
            for f in ("config.json", "summary.json", "replay.json"):
                if (run / f).exists():
                    shutil.copy2(run / f, target / f)
            for part in ("compare", "compare-posthoc", "latency"):
                for f in ("report.json", "latency.json"):
                    if (run / part / f).exists():
                        (target / part).mkdir(exist_ok=True)
                        shutil.copy2(run / part / f, target / part / f)


def extra(run: Path) -> dict:
    """Columns summarize.py does not write, from the run's summary.json (metric means over the measurement interval)."""
    s = json.loads((run / "summary.json").read_text())
    m = s["metrics"]

    def series(name: str) -> list[dict]:
        return [x for x in m.get(name, []) if x.get("mean") is not None]

    read = series("ticksReadPerSubtask")
    gc = series("flinkGcMsPerS")
    heap = series("flinkHeapBytes")
    windows = series("openWindows")
    states = series("emaStates")
    return {
        "run": json.loads((run / "config.json").read_text())["runId"],
        # D-37: the whole-job rate keeps the ~15 s ramp; this one is the read rate after warmupS
        "steadyReadPerSecond": sum(x["mean"] for x in read) if read else None,
        "gcMsPerS": sum(x["mean"] for x in gc) if gc else None,
        "heapMaxBytes": max(x["max"] for x in heap) if heap else None,
        "openWindowsMaxPerSubtask": max(x["max"] for x in windows) if windows else None,
        "emaStatesMaxPerSubtask": max(x["max"] for x in states) if states else None,
    }


def summarize(exps: dict[str, Path]) -> pl.DataFrame:
    frames = []
    for group, member in GROUPS.items():
        dirs = [str(d) for n, d in exps.items() if member(n)]
        if not dirs:
            continue
        subprocess.run([sys.executable, "bench/summarize.py", *dirs, "--out", str(OUT / group)], cwd=ROOT, check=True,
                       stdout=subprocess.DEVNULL)
        frames.append(pl.read_csv(OUT / group / "runs.csv", infer_schema_length=None).with_columns(group=pl.lit(group)))
    runs = pl.concat(frames, how="diagonal_relaxed")
    extras = [extra(r) for exp in exps.values() for r in sorted(exp.glob("r*")) if (r / "summary.json").exists()]
    return runs.join(pl.DataFrame(extras, infer_schema_length=None), on="run", how="left")


def fmt(values: list[float], scale: float = 1, digits: int = 2) -> str:
    # diagonal_relaxed concatenation turns a column that is all null in one group into strings
    v = [float(x) / scale for x in values if x not in (None, "")]
    if not v:
        return "–"
    mean = sum(v) / len(v)
    if len(v) == 1:
        return f"{mean:.{digits}f}"
    std = (sum((x - mean) ** 2 for x in v) / (len(v) - 1)) ** 0.5
    return f"{mean:.{digits}f} ± {std:.{digits}f}"


def table(runs: pl.DataFrame, group: str, columns: list[tuple[str, str, float, int]]) -> str:
    df = runs.filter(pl.col("group") == group)
    lines = ["| Experiment | n ok / runs | " + " | ".join(c[1] for c in columns) + " |",
             "|---|---|" + "---|" * len(columns)]
    for exp in sorted(df["experiment"].unique().to_list()):
        rows = df.filter(pl.col("experiment") == exp)
        ok = rows.filter(pl.col("status") == "COMPLETE")
        cells = [fmt(ok[c].to_list(), s, d) if c in ok.columns else "–" for c, _, s, d in columns]
        lines.append(f"| {exp.rsplit('-2026', 1)[0]} | {ok.height} / {rows.height} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


PRELOAD = [("jobRecordsPerSecond", "Job M rec/s", 1e6, 2), ("steadyReadPerSecond", "Post-warm-up M rec/s", 1e6, 2),
           ("jobDurationMs", "Job s", 1e3, 1), ("replayDurationMs", "Replay s", 1e3, 0),
           ("replayRecordsPerSecond", "Replay M rec/s", 1e6, 2),
           ("gcMsPerS", "GC ms/s", 1, 0), ("heapMaxBytes", "Heap max GiB", 2**30, 2),
           ("openWindowsMaxPerSubtask", "Open windows max/subtask", 1e3, 0),
           ("sourceBusyRatio", "Source busy", 1, 2), ("q1BusyRatio", "q1 busy", 1, 2),
           ("outputBusyRatio", "Output busy", 1, 2), ("sourceReadSkew", "Source read skew", 1, 2),
           ("q1ReadSkew", "q1 read skew", 1, 2), ("sourceBusySkew", "Source busy skew", 1, 2),
           ("taskmanagerCpuCores", "TM cores", 1, 1), ("kafkaCpuCores", "Kafka cores", 1, 2),
           ("emaResults", "Results", 1, 0), ("advisories", "Advisories", 1, 0), ("lateTicks", "Late", 1, 0)]
LIVE = [("replayRecordsPerSecond", "Replay k rec/s", 1e3, 0), ("latencyResults", "Measured", 1, 0),
        ("queueingP50Ms", "Queue p50", 1, 0), ("queueingP95Ms", "Queue p95", 1, 0), ("queueingP99Ms", "Queue p99", 1, 0),
        ("outputLatencyP50Ms", "Output p50", 1, 0), ("outputLatencyP95Ms", "Output p95", 1, 0),
        ("outputLatencyP99Ms", "Output p99", 1, 0), ("watermarkWaitP99Ms", "Watermark wait p99", 1, 0),
        ("watermarkLagWallP99Ms", "WM lag wall p99", 1, 0), ("taskmanagerCpuCores", "TM cores", 1, 2),
        ("lateTicks", "Late", 1, 0)]
PRELOAD_TRADERS = PRELOAD[:3] + [("outputBusyRatio", "Output busy", 1, 3)] + PRELOAD[-3:]


def resources(exps: dict[str, Path]) -> None:
    names = [n for n in exps if n.startswith("scale-")]
    fig, axes = plt.subplots(len(names), 1, figsize=(9, 2.2 * len(names)), sharex=True)
    for ax, name in zip(axes, names):
        run = sorted(exps[name].glob("r*"))[0]
        if not (run / "metrics.json").exists():
            continue
        metrics = json.loads((run / "metrics.json").read_text())["queries"]
        for series in metrics.get("cpuCores", {}).get("result", []):
            label = series["metric"].get("name", "")
            if not any(k in label for k in ("taskmanager", "kafka-1", "replay")):
                continue
            t0 = series["values"][0][0]
            ax.plot([float(t) - t0 for t, _ in series["values"]], [float(v) for _, v in series["values"]],
                    label=label.split("-")[-2] if "replay" not in label else "replay")
        ax.set_title(f"{name} {run.name}: CPU cores", fontsize=9)
        ax.legend(fontsize=7, loc="upper right")
    axes[-1].set_xlabel("s from first sample")
    fig.tight_layout()
    fig.savefig(OUT / "resources.png", dpi=110)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    exps = experiments()
    retain(exps)
    runs = summarize(exps)
    runs.write_csv(OUT / "runs.csv")
    resources(exps)
    sections = [("Scale (whole host, full-week preload)", table(runs, "scale", PRELOAD)),
                ("Exchanges (whole host, p4, 8 partitions)", table(runs, "exchanges", PRELOAD)),
                ("Traders (whole host, p4, 8 partitions)", table(runs, "traders", PRELOAD_TRADERS)),
                ("Variants (whole host, p4, 8 partitions)", table(runs, "variants", PRELOAD)),
                ("Live replay of 08 November from 09:00 (whole host, p4)", table(runs, "live", LIVE)),
                ("Calibration (lanes pinned to 4 cores / 8 threads)", table(runs, "calibration", PRELOAD[:2] + LIVE[2:5]))]
    (OUT / "tables.md").write_text("\n\n".join(f"### {t}\n\n{body}" for t, body in sections) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
