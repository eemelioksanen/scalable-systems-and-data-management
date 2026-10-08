"""Run the oracle over a set of day files and write its Parquet outputs and manifest."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import platform
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import polars as pl

from . import __version__, ema, parse, windows

# Bounds at which the manifest reports how many events a streaming run would reject.
REPORTED_BOUNDS_MS = (0, 1, 10, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 300_000, 3_600_000)

# D-18 fields first, in topic order; the rest are the oracle's own diagnostics.
EMA_COLUMNS = (
    "symbol", "secType", "windowEnd", "close", "ema38", "ema100",
    "exchange", "priceEvents", "closeEventTime", "closeSourceFile", "closeSourceLine",
)
ADVISORY_COLUMNS = ("symbol", "secType", "windowEnd", "type", "ema38", "ema100", "exchange")
ROW_GROUP_SIZE = 256 * 1024


@dataclass
class Config:
    inputs: list[Path]
    out: Path
    zone_offset: str = "+02:00"
    missing_trading_date: str = "system-date"
    watermark_bound_ms: int | None = None
    exclude_events: Path | None = None
    input_checksums: bool = True
    precision_check: bool = True
    command: list[str] = field(default_factory=list)


def _exchange(symbol: pl.Expr) -> pl.Expr:
    return symbol.str.extract(r"\.([^.]+)$")


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while block := f.read(16 << 20):
            h.update(block)
    return h.hexdigest()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(16 << 20):
            h.update(block)
    return h.hexdigest()


def _content_sha256(df: pl.DataFrame) -> str:
    """Checksum of the rows themselves, independent of Parquet encoding details."""
    h = hashlib.sha256()
    h.update(df.head(0).write_csv().encode())
    for part in df.iter_slices(500_000):
        h.update(part.write_csv(include_header=False).encode())
    return h.hexdigest()


def _git_revision() -> dict[str, object]:
    if rev := os.environ.get("ORACLE_GIT_REV"):
        return {"revision": rev, "dirty": os.environ.get("ORACLE_GIT_DIRTY") == "1"}
    here = Path(__file__).resolve().parent
    try:
        rev = subprocess.run(["git", "-C", str(here), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(here), "status", "--porcelain", "--", "."], capture_output=True, text=True, check=True).stdout.strip()
        return {"revision": rev, "dirty": bool(dirty)}
    except (OSError, subprocess.CalledProcessError):
        return {"revision": None, "dirty": None}


class _PeakAnonMemory:
    """Samples the process's anonymous resident memory (heap, not the memory-mapped input).

    ru_maxrss would also count the page-cache pages of the memory-mapped CSV, which says
    nothing about how much memory the oracle needs.
    """

    def __init__(self, interval: float = 0.1) -> None:
        self.peak_kb = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, args=(interval,), daemon=True)

    def _sample(self) -> None:
        try:
            for line in Path("/proc/self/status").read_text().splitlines():
                if line.startswith("RssAnon:"):
                    self.peak_kb = max(self.peak_kb, int(line.split()[1]))
        except OSError:
            pass

    def _loop(self, interval: float) -> None:
        while not self._stop.wait(interval):
            self._sample()

    def __enter__(self) -> "_PeakAnonMemory":
        self._sample()
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()
        self._sample()


def _load_exclusions(path: Path | None) -> pl.DataFrame:
    schema = {"sourceFile": pl.String, "sourceLine": pl.Int64}
    if path is None:
        return pl.DataFrame(schema=schema)
    df = pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path)
    return df.select(pl.col("sourceFile").cast(pl.String), pl.col("sourceLine").cast(pl.Int64)).unique()


def _write(df: pl.DataFrame, path: Path) -> dict[str, object]:
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True, row_group_size=ROW_GROUP_SIZE)
    return {"rows": df.height, "sha256": _sha256(path), "contentSha256": _content_sha256(df)}


def run(cfg: Config) -> dict[str, object]:
    with _PeakAnonMemory() as memory:
        manifest = _run(cfg)
    manifest["peakAnonMemoryMb"] = round(memory.peak_kb / 1024) if memory.peak_kb else None
    (cfg.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def _run(cfg: Config) -> dict[str, object]:
    t0 = time.monotonic()
    timings: dict[str, float] = {}
    zone_ms = parse.parse_zone_offset(cfg.zone_offset)
    inputs = parse.order_inputs(cfg.inputs)
    names = [p.name for p in inputs]
    if len(set(names)) != len(names):
        raise ValueError("input file names must be unique: they identify the source position")
    exclusions = _load_exclusions(cfg.exclude_events)
    cfg.out.mkdir(parents=True, exist_ok=True)

    per_file: dict[str, dict[str, int]] = {}
    candidates: list[pl.DataFrame] = []
    rejected: list[pl.DataFrame] = []
    positive_bounds: list[pl.Series] = []
    prior_max: int | None = None

    for idx, path in enumerate(inputs):
        t = time.monotonic()
        counts, events = parse.read_file(path, zone_ms, cfg.missing_trading_date)
        events, prior_max = windows.with_admission(events, prior_max)
        positive_bounds.append(events.filter(pl.col("minBoundMs") > 0)["minBoundMs"])

        excluded_lines = exclusions.filter(pl.col("sourceFile") == path.name)["sourceLine"]
        reason = pl.when(pl.col("sourceLine").cast(pl.Int64).is_in(excluded_lines.implode())).then(pl.lit("excluded"))
        if cfg.watermark_bound_ms is not None:
            reason = reason.when(pl.col("minBoundMs") > cfg.watermark_bound_ms).then(pl.lit("late"))
        events = events.with_columns(rejection=reason.otherwise(pl.lit(None, pl.String)))
        dropped = events.filter(pl.col("rejection").is_not_null())
        kept = events.filter(pl.col("rejection").is_null())

        counts["excluded"] = int((dropped["rejection"] == "excluded").sum())
        counts["lateRejected"] = int((dropped["rejection"] == "late").sum())
        counts["admitted"] = kept.height
        per_file[path.name] = counts
        if dropped.height:
            rejected.append(dropped.with_columns(sourceFile=pl.lit(path.name)))
        candidates.append(windows.file_candidates(kept, idx))
        timings[f"read:{path.name}"] = round(time.monotonic() - t, 3)
        del events, kept, dropped

    t = time.monotonic()
    closes = windows.select_closes(candidates, names)
    del candidates
    results, advisories = ema.compute(closes)
    results = results.with_columns(exchange=_exchange(pl.col("symbol"))).select(EMA_COLUMNS)
    advisories = advisories.with_columns(exchange=_exchange(pl.col("symbol"))).select(ADVISORY_COLUMNS)
    timings["ema"] = round(time.monotonic() - t, 3)

    precision = None
    if cfg.precision_check:
        t = time.monotonic()
        precision = asdict(ema.precision_check(results))
        timings["precisionCheck"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    trading_day = ((pl.col("windowEnd") - 1 + zone_ms) // 86_400_000).cast(pl.Int32).cast(pl.Date)
    crossover_summary = (
        advisories.group_by(tradingDay=trading_day, exchange=pl.col("exchange"), secType=pl.col("secType"), type=pl.col("type"))
        .agg(advisories=pl.len().cast(pl.Int64))
        .sort(["tradingDay", "exchange", "secType", "type"])
    )
    window_summary = (
        results.group_by(tradingDay=trading_day, exchange=pl.col("exchange"), secType=pl.col("secType"))
        .agg(windows=pl.len().cast(pl.Int64), symbols=pl.col("symbol").n_unique().cast(pl.Int64), priceEvents=pl.col("priceEvents").sum())
        .sort(["tradingDay", "exchange", "secType"])
    )
    adv_by_symbol = advisories.group_by("symbol").agg(
        buys=(pl.col("type") == "BUY").sum().cast(pl.Int64), sells=(pl.col("type") == "SELL").sum().cast(pl.Int64)
    )
    symbol_summary = (
        results.group_by("symbol")
        .agg(
            pl.col("exchange").first(),
            secTypes=pl.col("secType").unique().sort().str.join(""),
            windows=pl.len().cast(pl.Int64),
            priceEvents=pl.col("priceEvents").sum(),
            firstWindowEnd=pl.col("windowEnd").min(),
            lastWindowEnd=pl.col("windowEnd").max(),
        )
        .join(adv_by_symbol, on="symbol", how="left")
        .with_columns(pl.col("buys", "sells").fill_null(0))
        .sort("symbol")
    )
    input_counts = pl.DataFrame([{"sourceFile": name, **c} for name, c in per_file.items()])
    rejected_df = (
        pl.concat(rejected).select(
            "sourceFile", pl.col("sourceLine").cast(pl.Int64), "symbol", "secType", "eventTime", "windowEnd", "price", "minBoundMs", "rejection"
        )
        if rejected
        else pl.DataFrame(
            schema={"sourceFile": pl.String, "sourceLine": pl.Int64, "symbol": pl.String, "secType": pl.String, "eventTime": pl.Int64,
                    "windowEnd": pl.Int64, "price": pl.Float64, "minBoundMs": pl.Int64, "rejection": pl.String}
        )
    )

    outputs = {
        "ema_results.parquet": _write(results, cfg.out / "ema_results.parquet"),
        "advisories.parquet": _write(advisories, cfg.out / "advisories.parquet"),
        "crossover_summary.parquet": _write(crossover_summary, cfg.out / "crossover_summary.parquet"),
        "window_summary.parquet": _write(window_summary, cfg.out / "window_summary.parquet"),
        "symbol_summary.parquet": _write(symbol_summary, cfg.out / "symbol_summary.parquet"),
        "input_counts.parquet": _write(input_counts, cfg.out / "input_counts.parquet"),
        "rejected_events.parquet": _write(rejected_df, cfg.out / "rejected_events.parquet"),
    }
    timings["write"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    input_ids = []
    for path in inputs:
        entry: dict[str, object] = {"file": path.name, "fileDate": parse.file_date(path).isoformat(), "bytes": path.stat().st_size}
        if cfg.input_checksums:
            entry["md5"] = _md5(path)
        input_ids.append(entry)
    timings["inputChecksums"] = round(time.monotonic() - t, 3)

    bounds = pl.concat(positive_bounds) if positive_bounds else pl.Series(dtype=pl.Int64)
    total = {k: sum(c[k] for c in per_file.values()) for k in next(iter(per_file.values()))}
    admission = {
        "model": "price events in source order, per-event watermark = max event time seen - bound - 1",
        "eventsWithPositiveMinBound": bounds.len(),
        "smallestBoundWithNoLateEventsMs": int(bounds.max()) if bounds.len() else 0,
        "positiveMinBoundQuantilesMs": {
            q: int(bounds.quantile(float(q), "higher")) for q in ("0.5", "0.9", "0.99", "0.999")
        } if bounds.len() else {},
        "lateEventsByBoundMs": {str(b): int((bounds > b).sum()) for b in REPORTED_BOUNDS_MS},
    }
    type_counts = dict(advisories.group_by("type").len().iter_rows())

    manifest = {
        "tool": "ema_oracle",
        "version": __version__,
        "git": _git_revision(),
        "command": cfg.command,
        "createdUtc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "environment": {"python": platform.python_version(), "polars": pl.__version__, "platform": platform.platform()},
        "parameters": {
            "zoneOffset": cfg.zone_offset,
            "missingTradingDate": cfg.missing_trading_date,
            "watermarkBoundMs": cfg.watermark_bound_ms,
            "excludeEvents": str(cfg.exclude_events) if cfg.exclude_events else None,
            "excludedEventsListed": exclusions.height,
            "windowMs": windows.WINDOW_MS,
            "alpha38": ema.ALPHA_SHORT,
            "alpha100": ema.ALPHA_LONG,
            "initialEma": 0.0,
        },
        "inputs": input_ids,
        "counts": {"total": total, "perFile": per_file},
        "admission": admission,
        "results": {
            "windows": results.height,
            "symbols": results["symbol"].n_unique(),
            "symbolsWithSeveralSecTypes": int(symbol_summary.filter(pl.col("secTypes").str.len_chars() > 1).height),
            "advisories": {"BUY": type_counts.get("BUY", 0), "SELL": type_counts.get("SELL", 0)},
            "firstWindowEnd": results["windowEnd"].min(),
            "lastWindowEnd": results["windowEnd"].max(),
        },
        "precision": precision,
        "outputs": outputs,
        "timingsSeconds": {**timings, "total": round(time.monotonic() - t0, 3)},
    }
    return manifest
