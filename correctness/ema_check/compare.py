"""Compare an export of the job's output with an oracle run (D-18).

The EMA check joins on (symbol, windowEnd): `secType` and `close` must be equal, and each EMA
within an absolute tolerance. The advisory check compares (symbol, windowEnd, type)
identities. Duplicates are found on each side before anything is joined or deduplicated.

Every difference is classified, so that agreement cannot come from discarded results:

- `lateRejection`: the job (`--job-late-events`) or the oracle (its `rejected_events.parquet`)
  rejected an event of this symbol in this window or an earlier one, which changes this window
  and every later EMA of the symbol. `rejectedBy` says which side.
- `precisionDrift`: same close, EMAs apart by at most `drift_ulps` units in the last place.
  Neither side is wrong: binary64 drift alone exceeds 1e-9 on some windows (D-27).
- `pipelineLoss`: the oracle has the window, the job does not, and no late event explains it.
- `unexplained`: anything else. Either side may be wrong; the oracle's close source position
  is in the table to check it against the input.

The gate passes only if there are no differences of any class, no duplicates, the export is
complete, both sides used the same zone offset, and the job rejected exactly as many events
as the oracle did.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import polars as pl

KEY = ["symbol", "windowEnd"]
EMA_FIELDS = ["symbol", "secType", "windowEnd", "close", "ema38", "ema100"]
ADVISORY_KEY = ["symbol", "windowEnd", "type"]
TOLERANCE = 1e-9
# Each side's binary64 recurrence stays within 33 ulps of exact over the week (D-27).
DRIFT_ULPS = 66


@dataclass
class Inputs:
    oracle: Path
    job: Path
    out: Path
    job_late_ticks: int | None = None
    job_late_events: Path | None = None
    tolerance: float = TOLERANCE
    drift_ulps: float = DRIFT_ULPS


def _manifest(directory: Path) -> dict:
    path = directory / "manifest.json"
    return json.loads(path.read_text()) if path.exists() else {}


def _read_table(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path)


def _duplicates(df: pl.DataFrame, key: list[str], side: str) -> pl.DataFrame:
    return df.filter(pl.len().over(key) > 1).with_columns(side=pl.lit(side)).sort(key)


def _ulps(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    """|a - b| in units in the last place of the larger magnitude."""
    exponent = pl.max_horizontal(a.abs(), b.abs()).log(2).floor()
    return pl.when(a == b).then(0.0).otherwise((a - b).abs() / (pl.lit(2.0) ** (exponent - 52)))


def _first_rejected(job_late: pl.DataFrame | None, oracle_rejected: pl.DataFrame | None) -> pl.DataFrame:
    """Per symbol, the first window in which the job and the oracle rejected an event."""
    sides = []
    for side, df in (("job", job_late), ("oracle", oracle_rejected)):
        if df is not None and not df.is_empty():
            sides.append(df.select(pl.col("symbol").cast(pl.String), pl.col("windowEnd").cast(pl.Int64), side=pl.lit(side)))
    firsts = pl.concat(sides) if sides else pl.DataFrame(schema={"symbol": pl.String, "windowEnd": pl.Int64, "side": pl.String})
    agg = firsts.group_by("symbol").agg(
        firstJobRejection=pl.col("windowEnd").filter(pl.col("side") == "job").min(),
        firstOracleRejection=pl.col("windowEnd").filter(pl.col("side") == "oracle").min(),
    )
    return agg.with_columns(pl.col("firstJobRejection", "firstOracleRejection").cast(pl.Int64))


def _classify(df: pl.DataFrame, rejections: pl.DataFrame, drift: pl.Expr) -> pl.DataFrame:
    by_job = pl.col("firstJobRejection") <= pl.col("windowEnd")
    by_oracle = pl.col("firstOracleRejection") <= pl.col("windowEnd")
    return (
        df.join(rejections, on="symbol", how="left")
        .with_columns(
            rejectedBy=pl.when(by_job & by_oracle).then(pl.lit("both"))
            .when(by_job).then(pl.lit("job"))
            .when(by_oracle).then(pl.lit("oracle")),
        )
        .with_columns(
            difference=pl.when(pl.col("rejectedBy").is_not_null()).then(pl.lit("lateRejection"))
            .when(drift).then(pl.lit("precisionDrift"))
            .when(pl.col("kind") == "oracleOnly").then(pl.lit("pipelineLoss"))
            .otherwise(pl.lit("unexplained")),
        )
        .drop("firstJobRejection", "firstOracleRejection")
    )


def _ema_differences(oracle: pl.DataFrame, job: pl.DataFrame, late: pl.DataFrame, cfg: Inputs) -> pl.DataFrame:
    diagnostics = [c for c in ("closeSourceFile", "closeSourceLine") if c in oracle.columns]
    o = oracle.select(*KEY, *[pl.col(c).alias(f"{c}Oracle") for c in ("secType", "close", "ema38", "ema100")], *diagnostics, _o=pl.lit(True))
    j = job.select(*KEY, *[pl.col(c).alias(f"{c}Job") for c in ("secType", "close", "ema38", "ema100")], _j=pl.lit(True))
    joined = o.join(j, on=KEY, how="full", coalesce=True).with_columns(
        absDiffEma38=(pl.col("ema38Oracle") - pl.col("ema38Job")).abs(),
        absDiffEma100=(pl.col("ema100Oracle") - pl.col("ema100Job")).abs(),
        ulpsEma38=_ulps(pl.col("ema38Oracle"), pl.col("ema38Job")),
        ulpsEma100=_ulps(pl.col("ema100Oracle"), pl.col("ema100Job")),
    )
    kind = (
        pl.when(pl.col("_j").is_null()).then(pl.lit("oracleOnly"))
        .when(pl.col("_o").is_null()).then(pl.lit("jobOnly"))
        .when(pl.col("secTypeOracle").ne_missing(pl.col("secTypeJob"))).then(pl.lit("secType"))
        .when(pl.col("closeOracle").ne_missing(pl.col("closeJob"))).then(pl.lit("close"))
        .when(
            pl.col("absDiffEma38").is_null() | pl.col("absDiffEma100").is_null()
            | (pl.col("absDiffEma38") > cfg.tolerance) | (pl.col("absDiffEma100") > cfg.tolerance)
        ).then(pl.lit("ema"))
    )
    differences = joined.with_columns(kind=kind).filter(pl.col("kind").is_not_null()).drop("_o", "_j")
    drift = (pl.col("kind") == "ema") & (pl.col("ulpsEma38") <= cfg.drift_ulps) & (pl.col("ulpsEma100") <= cfg.drift_ulps)
    return _classify(differences, late, drift).sort(KEY)


def _advisory_differences(oracle: pl.DataFrame, job: pl.DataFrame, late: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    o, j = oracle.select(ADVISORY_KEY).unique(), job.select(ADVISORY_KEY).unique()
    common = o.join(j, on=ADVISORY_KEY, how="semi").height
    differences = pl.concat([
        o.join(j, on=ADVISORY_KEY, how="anti").with_columns(kind=pl.lit("oracleOnly")),
        j.join(o, on=ADVISORY_KEY, how="anti").with_columns(kind=pl.lit("jobOnly")),
    ])
    return _classify(differences, late, pl.lit(False)).sort(ADVISORY_KEY), common


def _ratio(num: int, den: int) -> float | None:
    return num / den if den else None


def _counts(df: pl.DataFrame, column: str) -> dict[str, int]:
    return {k: v for k, v in sorted(df.group_by(column).len().iter_rows())}


def _export_problems(manifest: dict) -> list[str]:
    if not manifest:
        return ["the job export has no manifest.json, so its completeness is unknown"]
    problems = []
    exported = {info.get("file"): (topic, info) for topic, info in manifest.get("topics", {}).items()}
    for file in ("ema_results.parquet", "advisories.parquet"):
        if file not in exported:
            problems.append(f"the export manifest does not cover {file}")
            continue
        topic, info = exported[file]
        if not info.get("complete"):
            problems.append(f"the export of {topic} is incomplete: {info.get('problem', 'see its manifest')}")
    for topic, info in manifest.get("topics", {}).items():
        if info.get("file") == "late_events.parquet" and not info.get("complete"):
            problems.append(f"the export of {topic} is incomplete: {info.get('problem', 'see its manifest')}")
    return problems


def _late_identity_problems(events: pl.DataFrame, rejected: pl.DataFrame | None, count: int | None) -> list[str]:
    key = ["sourceFile", "sourceLine", "symbol", "windowEnd"]
    if count is not None and events.height != count:
        return ["late-event export length differs from the job's lateTicks"]
    if not set(key).issubset(events.columns):
        return ["late events need sourceFile, sourceLine, symbol and windowEnd to prove rejection identities"]
    if events.select(pl.any_horizontal(pl.col(key).is_null()).any()).item():
        return ["late events contain null identities"]
    if events.select("sourceFile", "sourceLine").is_duplicated().any():
        return ["late-event export contains duplicate source identities"]
    if events.is_empty() and (rejected is None or rejected.is_empty()):
        return []
    if rejected is None or not set(key).issubset(rejected.columns):
        return ["oracle rejected_events.parquet is missing rejection identities"]
    if (events.join(rejected, on=key, how="anti").height
            or rejected.join(events, on=key, how="anti").height):
        return ["job and oracle rejected different source events"]
    return []


def compare(cfg: Inputs) -> dict:
    oracle_manifest, job_manifest = _manifest(cfg.oracle), _manifest(cfg.job)
    problems = _export_problems(job_manifest)

    oracle_zone = oracle_manifest.get("parameters", {}).get("zoneOffset")
    job_zone = job_manifest.get("zoneOffset")
    if oracle_zone is None or job_zone is None or oracle_zone != job_zone:
        problems.append(f"zone offsets must be recorded and equal (D-3): oracle {oracle_zone}, job {job_zone}")

    totals = oracle_manifest.get("counts", {}).get("total", {})
    oracle_rejected_count = totals.get("excluded", 0) + totals.get("lateRejected", 0)
    if cfg.job_late_ticks is None:
        problems.append("the job's lateTicks was not given, so its rejections cannot be matched to the oracle's")
    elif cfg.job_late_ticks != oracle_rejected_count:
        problems.append(
            f"the job rejected {cfg.job_late_ticks} late events and the oracle {oracle_rejected_count}: "
            "compare against an oracle run that rejects the same events"
        )

    late_events = _read_table(cfg.job_late_events) if cfg.job_late_events else None
    oracle_rejected_path = cfg.oracle / "rejected_events.parquet"
    oracle_rejected = pl.read_parquet(oracle_rejected_path) if oracle_rejected_path.exists() else None
    if late_events is not None:
        problems += _late_identity_problems(late_events, oracle_rejected, cfg.job_late_ticks)
    elif cfg.job_late_ticks:
        problems.append("nonzero lateTicks requires --job-late-events to prove rejection identities")
    late = _first_rejected(late_events, oracle_rejected)

    oracle = pl.read_parquet(cfg.oracle / "ema_results.parquet")
    job = pl.read_parquet(cfg.job / "ema_results.parquet")
    oracle_adv = pl.read_parquet(cfg.oracle / "advisories.parquet")
    job_adv = pl.read_parquet(cfg.job / "advisories.parquet")

    duplicates = pl.concat([
        _duplicates(oracle.select(EMA_FIELDS), KEY, "oracle"),
        _duplicates(job.select(EMA_FIELDS), KEY, "job"),
    ])
    adv_duplicates = pl.concat([
        _duplicates(oracle_adv.select(ADVISORY_KEY), ADVISORY_KEY, "oracle"),
        _duplicates(job_adv.select(ADVISORY_KEY), ADVISORY_KEY, "job"),
    ])
    ema_diff = _ema_differences(oracle, job, late, cfg)
    adv_diff, adv_common = _advisory_differences(oracle_adv, job_adv, late)

    cfg.out.mkdir(parents=True, exist_ok=True)
    ema_diff.write_parquet(cfg.out / "ema_differences.parquet")
    adv_diff.write_parquet(cfg.out / "advisory_differences.parquet")
    duplicates.write_parquet(cfg.out / "ema_duplicates.parquet")
    adv_duplicates.write_parquet(cfg.out / "advisory_duplicates.parquet")

    oracle_ids = oracle_adv.select(ADVISORY_KEY).n_unique()
    job_ids = job_adv.select(ADVISORY_KEY).n_unique()

    def max_of(column: str) -> float | None:
        value = ema_diff.filter(pl.col("kind") == "ema")[column].max()
        return None if value is None or math.isnan(value) else value

    report = {
        "pass": not problems and ema_diff.is_empty() and adv_diff.is_empty() and duplicates.is_empty() and adv_duplicates.is_empty(),
        "problems": problems,
        "parameters": {"tolerance": cfg.tolerance, "driftUlps": cfg.drift_ulps},
        "oracle": {
            "dir": str(cfg.oracle),
            "command": oracle_manifest.get("command"),
            "zoneOffset": oracle_zone,
            "watermarkBoundMs": oracle_manifest.get("parameters", {}).get("watermarkBoundMs"),
            "excludeEvents": oracle_manifest.get("parameters", {}).get("excludeEvents"),
            "excluded": totals.get("excluded"),
            "lateRejected": totals.get("lateRejected"),
        },
        "job": {
            "dir": str(cfg.job),
            "zoneOffset": job_zone,
            "lateTicks": cfg.job_late_ticks,
            "lateEventsListed": None if late_events is None else late_events.height,
            "topics": job_manifest.get("topics"),
        },
        "ema": {
            "oracleRows": oracle.height,
            "jobRows": job.height,
            "differences": ema_diff.height,
            "byKind": _counts(ema_diff, "kind"),
            "byClass": _counts(ema_diff, "difference"),
            "symbolsWithDifferences": ema_diff["symbol"].n_unique(),
            "duplicateRows": {"oracle": int((duplicates["side"] == "oracle").sum()), "job": int((duplicates["side"] == "job").sum())},
            "maxAbsDiffBeyondTolerance": {"ema38": max_of("absDiffEma38"), "ema100": max_of("absDiffEma100")},
            "maxUlpsBeyondTolerance": {"ema38": max_of("ulpsEma38"), "ema100": max_of("ulpsEma100")},
        },
        "advisories": {
            "oracle": oracle_ids,
            "job": job_ids,
            "common": adv_common,
            "precision": _ratio(adv_common, job_ids),
            "recall": _ratio(adv_common, oracle_ids),
            "byKind": _counts(adv_diff, "kind"),
            "byClass": _counts(adv_diff, "difference"),
            "duplicateRows": {"oracle": int((adv_duplicates["side"] == "oracle").sum()), "job": int((adv_duplicates["side"] == "job").sum())},
        },
    }
    (cfg.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
