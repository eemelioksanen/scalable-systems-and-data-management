"""Compares the #11 runs the harness could not compare with an oracle (bench/run.py refuses one with a selection).

- Exchange subsets: the full-week admit-all oracle cut to the selected exchanges. Each symbol's EMA depends only on its
  own events, and an admit-all preload rejects none, so the cut oracle is the subset's reference.
- Traders: the same oracle cut to the union of the subscribed symbols. The subscription filter runs after q1-ema, so
  this checks both the EMAs and what the filter published.
- Live slices: an oracle at the job's 1 s bound over a copy of Monday's first 3,000,000 data rows, which is exactly
  what the replayer's --limit sent.

Each run's export stays where bench/run.py left it; the comparison goes to <run>/compare-posthoc/. Run after the
matrix, so the comparisons do not share the host with a measurement:

    bench/.venv/bin/python scripts/validation/issue11/posthoc_correctness.py
"""

import json
import shutil
import subprocess
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / "bench/results/raw"
ORACLES = ROOT / "oracle/out"
MONDAY = ROOT / "data/daily/debs2022-gc-trading-day-08-11-21.csv"
SLICE_ROWS = 3_000_000


def cut_oracle(name: str, keep: pl.Expr, source: Path = ORACLES / "week") -> Path:
    out = ORACLES / f"issue11-{name}"
    if (out / "manifest.json").exists():
        return out
    out.mkdir(parents=True, exist_ok=True)
    for table in ("ema_results", "advisories", "rejected_events"):
        pl.read_parquet(source / f"{table}.parquet").filter(keep).write_parquet(out / f"{table}.parquet")
    manifest = json.loads((source / "manifest.json").read_text())
    rejected = pl.read_parquet(out / "rejected_events.parquet").height
    total = manifest["counts"]["total"]
    total["excluded"], total["lateRejected"] = 0, rejected
    manifest["issue11Cut"] = {"from": str(source.relative_to(ROOT)), "keep": str(keep),
                              "emaResults": pl.read_parquet(out / "ema_results.parquet").height,
                              "advisories": pl.read_parquet(out / "advisories.parquet").height}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return out


def slice_oracle() -> Path:
    out = ORACLES / "issue11-mon-slice3m-bound1s"
    if (out / "manifest.json").exists():
        return out
    target = ROOT / "data/issue11" / MONDAY.name
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with MONDAY.open() as src, target.open("w") as dst:
        for line in src:  # comment and header lines pass through; the replayer's --limit counts data rows only
            dst.write(line)
            if not line.startswith("#") and not line.startswith("ID,"):
                rows += 1
                if rows == SLICE_ROWS:
                    break
    subprocess.run(["oracle/run.sh", "--out", f"/out/{out.name}", "--watermark-bound-ms", "1000",
                    str(target.relative_to(ROOT))], cwd=ROOT, check=True)
    return out


def latest(name: str) -> Path | None:
    dirs = sorted(RAW.glob(f"{name}-20[0-9][0-9][0-9][0-9][0-9][0-9]T*"))
    return dirs[-1] if dirs else None


def compare(run: Path, oracle: Path) -> dict:
    summary = json.loads((run / "summary.json").read_text())
    late = summary["counters"]["lateTicks"] if summary["reconciliation"]["countersFinal"]["lateTicks"] else None
    rid = f"posthoc-{run.parent.name}-{run.name}"
    args = ["correctness/run.sh", "compare", "--oracle", str(oracle.relative_to(ROOT)), "--job", str(run / "export"),
            "--out", f"/out/bench/{rid}"]
    if late is not None:
        args += ["--job-late-ticks", str(late), "--job-late-events", str(run / "export" / "late_events.parquet")]
    subprocess.run(args, cwd=ROOT, check=False)
    scratch = ROOT / "correctness/out/bench" / rid
    target = run / "compare-posthoc"
    shutil.rmtree(target, ignore_errors=True)
    shutil.move(str(scratch), str(target))
    report = json.loads((target / "report.json").read_text())
    return {"run": str(run.relative_to(ROOT)), "oracle": str(oracle.relative_to(ROOT)),
            "verdict": "pass" if report.get("pass") else "fail", "problems": report.get("problems"),
            **{k: v for k, v in report.items() if k in ("counts", "differences", "ema", "advisories")}}


def main() -> None:
    plans: list[tuple[str, Path]] = []
    for name, exchanges in (("lane-exch-fr", ["FR"]), ("lane-exch-fr-nl", ["FR", "NL"])):
        plans.append((name, cut_oracle(f"week-{'-'.join(exchanges).lower()}", pl.col("symbol").str.extract(r"\.([^.]+)$").is_in(exchanges))))
    for cfg in sorted((ROOT / "bench/configs/issue11/lane").glob("lane-traders-*.json")):
        subs = json.loads(cfg.read_text())["subscriptions"]
        symbols = sorted({s for sub in subs for s in sub["symbols"]})
        plans.append((cfg.stem, cut_oracle(f"week-{cfg.stem.removeprefix('lane-')}", pl.col("symbol").is_in(symbols))))
    sliced = slice_oracle()
    plans += [("lane-live-1x-slice", sliced), ("lane-live-10x-slice", sliced)]

    results = []
    for name, oracle in plans:
        exp = latest(name)
        if exp is None:
            results.append({"experiment": name, "verdict": "missing"})
            continue
        for run in sorted(exp.glob("r*")):
            if json.loads((run / "config.json").read_text()).get("status") != "COMPLETE":
                results.append({"run": str(run.relative_to(ROOT)), "verdict": "not COMPLETE, skipped"})
                continue
            results.append(compare(run, oracle))
            print(json.dumps(results[-1]), flush=True)
    (RAW / "issue11-posthoc-correctness.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
