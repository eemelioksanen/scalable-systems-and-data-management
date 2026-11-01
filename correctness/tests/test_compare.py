"""The comparer on fixtures/golden/ and on planted faults."""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl
import pytest

from ema_check.compare import Inputs, compare

GOLDEN = Path(__file__).resolve().parents[2] / "fixtures" / "golden"


def _golden() -> tuple[pl.DataFrame, pl.DataFrame]:
    results, advisories = [], []
    for name in ("synthetic", "real"):
        fixture = json.loads((GOLDEN / f"{name}.json").read_text())
        ident = {"symbol": fixture["symbol"], "secType": fixture["secType"]}
        results += [{**ident, **{k: w[k] for k in ("windowEnd", "close", "ema38", "ema100")}} for w in fixture["windows"]]
        advisories += [{**ident, **{k: a[k] for k in ("windowEnd", "type", "ema38", "ema100")}} for a in fixture["advisories"]]
    return pl.DataFrame(results), pl.DataFrame(advisories)


def _oracle_dir(path: Path, results: pl.DataFrame, advisories: pl.DataFrame, rejected: int = 0) -> Path:
    path.mkdir()
    results.write_parquet(path / "ema_results.parquet")
    advisories.write_parquet(path / "advisories.parquet")
    manifest = {"parameters": {"zoneOffset": "Z"}, "counts": {"total": {"excluded": 0, "lateRejected": rejected}}}
    (path / "manifest.json").write_text(json.dumps(manifest))
    return path


def _job_dir(path: Path, results: pl.DataFrame, advisories: pl.DataFrame, zone: str = "Z", complete: bool = True) -> Path:
    path.mkdir()
    results.write_parquet(path / "ema_results.parquet")
    advisories.write_parquet(path / "advisories.parquet")
    topics = {
        "ema-results": {"file": "ema_results.parquet", "complete": complete},
        "advisories": {"file": "advisories.parquet", "complete": True},
    }
    (path / "manifest.json").write_text(json.dumps({"zoneOffset": zone, "topics": topics}))
    return path


def _run(tmp_path: Path, job_results: pl.DataFrame, job_advisories: pl.DataFrame, late_ticks: int | None = 0, **kw) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    results, advisories = _golden()
    oracle = _oracle_dir(tmp_path / "oracle", results, advisories, kw.pop("oracle_rejected", 0))
    job = _job_dir(tmp_path / "job", job_results, job_advisories, kw.pop("zone", "Z"), kw.pop("complete", True))
    report = compare(Inputs(oracle=oracle, job=job, out=tmp_path / "cmp", job_late_ticks=late_ticks, **kw))
    return (
        report,
        pl.read_parquet(tmp_path / "cmp" / "ema_differences.parquet"),
        pl.read_parquet(tmp_path / "cmp" / "advisory_differences.parquet"),
    )


def _set(df: pl.DataFrame, symbol: str, window_end: int, **values) -> pl.DataFrame:
    hit = (pl.col("symbol") == symbol) & (pl.col("windowEnd") == window_end)
    return df.with_columns(**{k: pl.when(hit).then(v).otherwise(pl.col(k)) for k, v in values.items()})


@pytest.fixture
def golden():
    return _golden()


def test_identical_output_passes(tmp_path, golden):
    report, ema, adv = _run(tmp_path, *golden)
    assert report["pass"], report["problems"]
    assert ema.is_empty() and adv.is_empty()
    assert report["advisories"]["precision"] == report["advisories"]["recall"] == 1.0


def test_ema_within_tolerance_passes(tmp_path, golden):
    results, advisories = golden
    report, _, _ = _run(tmp_path, results.with_columns(pl.col("ema38") + 5e-10), advisories)
    assert report["pass"]


def test_changed_close(tmp_path, golden):
    results, advisories = golden
    w = results["windowEnd"][3]
    report, ema, _ = _run(tmp_path, _set(results, "SYNTH.TEST", w, close=pl.col("close") + 0.01), advisories)
    assert not report["pass"]
    row = ema.row(0, named=True)
    assert (row["symbol"], row["windowEnd"], row["kind"], row["difference"]) == ("SYNTH.TEST", w, "close", "unexplained")
    assert row["closeJob"] - row["closeOracle"] == pytest.approx(0.01)


def test_ema_outside_tolerance(tmp_path, golden):
    results, advisories = golden
    w = results.filter(pl.col("symbol") == "A0X7J3.ETR")["windowEnd"][5]
    report, ema, _ = _run(tmp_path, _set(results, "A0X7J3.ETR", w, ema100=pl.col("ema100") + 1e-6), advisories)
    assert not report["pass"]
    assert ema.select("symbol", "windowEnd", "kind").rows() == [("A0X7J3.ETR", w, "ema")]
    assert ema["absDiffEma100"][0] == pytest.approx(1e-6)
    assert report["ema"]["maxAbsDiffBeyondTolerance"]["ema100"] == pytest.approx(1e-6)


def test_missing_window(tmp_path, golden):
    results, advisories = golden
    w = results["windowEnd"][10]
    report, ema, _ = _run(tmp_path, results.filter(~((pl.col("symbol") == "SYNTH.TEST") & (pl.col("windowEnd") == w))), advisories)
    assert not report["pass"]
    assert ema.select("symbol", "windowEnd", "kind", "difference").rows() == [("SYNTH.TEST", w, "oracleOnly", "pipelineLoss")]


def test_duplicate_record(tmp_path, golden):
    results, advisories = golden
    report, _, _ = _run(tmp_path, pl.concat([results, results.slice(7, 1)]), advisories)
    assert not report["pass"]
    assert report["ema"]["duplicateRows"] == {"oracle": 0, "job": 2}
    dups = pl.read_parquet(tmp_path / "cmp" / "ema_duplicates.parquet")
    assert dups.select("side", "symbol", "windowEnd").unique().rows() == [("job", results["symbol"][7], results["windowEnd"][7])]


def test_spurious_advisory(tmp_path, golden):
    results, advisories = golden
    extra = advisories.slice(0, 1).with_columns(windowEnd=results["windowEnd"][4], type=pl.lit("SELL"))
    report, _, adv = _run(tmp_path, results, pl.concat([advisories, extra]))
    assert not report["pass"]
    assert adv.select("symbol", "windowEnd", "type", "kind").rows() == [("SYNTH.TEST", results["windowEnd"][4], "SELL", "jobOnly")]
    assert report["advisories"]["precision"] == pytest.approx(3 / 4)
    assert report["advisories"]["recall"] == 1.0


@pytest.mark.parametrize("column", ["secType", "close", "ema38", "ema100"])
def test_null_value(tmp_path, golden, column):
    results, advisories = golden
    w = results["windowEnd"][2]
    report, ema, _ = _run(tmp_path, _set(results, "SYNTH.TEST", w, **{column: pl.lit(None)}), advisories)
    assert not report["pass"]
    assert ema.select("symbol", "windowEnd").rows() == [("SYNTH.TEST", w)]


def test_precision_drift_is_classified(tmp_path, golden):
    results, advisories = golden
    w = results["windowEnd"][20]
    drifted = _set(results, "SYNTH.TEST", w, ema38=pl.col("ema38").map_elements(lambda x: math.nextafter(math.nextafter(x, math.inf), math.inf), return_dtype=pl.Float64))
    report, ema, _ = _run(tmp_path, drifted, advisories, tolerance=0.0)
    assert not report["pass"]
    assert ema.select("kind", "difference", "ulpsEma38").rows() == [("ema", "precisionDrift", 2.0)]


def test_late_rejection_explains_later_windows(tmp_path, golden):
    results, advisories = golden
    synth = results.filter(pl.col("symbol") == "SYNTH.TEST")
    w = synth["windowEnd"][30]
    job = results.filter(~((pl.col("symbol") == "SYNTH.TEST") & (pl.col("windowEnd") == w))).with_columns(
        ema38=pl.when((pl.col("symbol") == "SYNTH.TEST") & (pl.col("windowEnd") > w)).then(pl.col("ema38") + 1e-3).otherwise(pl.col("ema38"))
    )
    late = tmp_path / "late.csv"
    pl.DataFrame({"symbol": ["SYNTH.TEST"], "windowEnd": [w]}).write_csv(late)

    report, ema, _ = _run(tmp_path, job, advisories, late_ticks=1, oracle_rejected=0, job_late_events=late)
    assert ema.select("difference", "rejectedBy").unique().rows() == [("lateRejection", "job")]
    assert ema.height == 50 - 30
    assert any("rejected 1 late events and the oracle 0" in p for p in report["problems"])


def test_oracle_rejection_explains_job_only_windows(tmp_path, golden):
    results, advisories = golden
    w = results.filter(pl.col("symbol") == "SYNTH.TEST")["windowEnd"][40]
    oracle_results = results.filter(~((pl.col("symbol") == "SYNTH.TEST") & (pl.col("windowEnd") >= w)))
    oracle = _oracle_dir(tmp_path / "oracle", oracle_results, advisories, rejected=1)
    pl.DataFrame({"sourceFile": ["f-08-11-21.csv"], "sourceLine": [9], "symbol": ["SYNTH.TEST"], "windowEnd": [w]}).write_parquet(oracle / "rejected_events.parquet")
    job = _job_dir(tmp_path / "job", results, advisories)

    report = compare(Inputs(oracle=oracle, job=job, out=tmp_path / "cmp", job_late_ticks=0))
    ema = pl.read_parquet(tmp_path / "cmp" / "ema_differences.parquet")
    assert ema.select("kind", "difference", "rejectedBy").unique().rows() == [("jobOnly", "lateRejection", "oracle")]
    assert ema.height == 10
    assert not report["pass"]


@pytest.mark.parametrize(
    ("kw", "problem"),
    [
        ({"zone": "+02:00"}, "zone offsets"),
        ({"late_ticks": None}, "lateTicks was not given"),
        ({"complete": False}, "export of ema-results is incomplete"),
    ],
)
def test_input_problems_fail_the_gate(tmp_path, golden, kw, problem):
    report, _, _ = _run(tmp_path, *golden, **kw)
    assert not report["pass"]
    assert any(problem in p for p in report["problems"])


@pytest.mark.parametrize("lines,expected", [([7], True), ([8], False), ([7, 7], False), ([None], False)])
def test_late_event_identities_must_match(tmp_path, golden, lines, expected):
    results, advisories = golden
    oracle = _oracle_dir(tmp_path / "oracle", results, advisories, rejected=len(lines))
    job = _job_dir(tmp_path / "job", results, advisories)
    rejected = pl.DataFrame({"sourceFile": ["day.csv"], "sourceLine": [7], "symbol": ["S.NL"], "windowEnd": [300000]})
    rejected.write_parquet(oracle / "rejected_events.parquet")
    events = pl.DataFrame({"sourceFile": ["day.csv"] * len(lines), "sourceLine": lines,
                           "symbol": ["S.NL"] * len(lines), "windowEnd": [300000] * len(lines)},
                          schema={"sourceFile": pl.String, "sourceLine": pl.Int64, "symbol": pl.String, "windowEnd": pl.Int64})
    path = job / "late_events.parquet"
    events.write_parquet(path)
    report = compare(Inputs(oracle=oracle, job=job, out=tmp_path / "cmp", job_late_ticks=len(lines), job_late_events=path))
    assert report["pass"] == expected, report["problems"]


def test_nonzero_late_count_requires_source_identities(tmp_path, golden):
    report, _, _ = _run(tmp_path, *golden, late_ticks=1, oracle_rejected=1)
    assert not report["pass"]
    assert any("requires --job-late-events" in problem for problem in report["problems"])


def test_missing_late_count_is_not_reported_as_length_mismatch(tmp_path, golden):
    path = tmp_path / "late.parquet"
    pl.DataFrame(schema={"sourceFile": pl.String, "sourceLine": pl.Int64,
                         "symbol": pl.String, "windowEnd": pl.Int64}).write_parquet(path)
    report, _, _ = _run(tmp_path, *golden, late_ticks=None, job_late_events=path)
    assert not report["pass"]
    assert any("lateTicks was not given" in problem for problem in report["problems"])
    assert not any("length differs" in problem for problem in report["problems"])
