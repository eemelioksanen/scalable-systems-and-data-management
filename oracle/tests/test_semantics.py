"""The oracle against the hand-derived fixtures in fixtures/semantics/ (see derivation.md there)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from ema_oracle.run import Config, run

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "semantics"
DAY1, DAY2 = "trading-day-08-11-21.csv", "trading-day-09-11-21.csv"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text())
# Fixture prices are at most 100, where binary64 carries ~1e-14; far tighter than D-18's 1e-9.
EMA_TOL = 1e-12


def oracle(tmp_path: Path, name: str = "out", **kw) -> tuple[dict, Path]:
    out = tmp_path / name
    kw.setdefault("zone_offset", EXPECTED["zoneOffset"])
    manifest = run(Config(inputs=[FIXTURES / DAY2, FIXTURES / DAY1], out=out, input_checksums=False, **kw))
    return manifest, out


@pytest.fixture(scope="module")
def default_run(tmp_path_factory):
    return oracle(tmp_path_factory.mktemp("default"))


def test_windows_closes_and_emas(default_run):
    _, out = default_run
    got = pl.read_parquet(out / "ema_results.parquet").sort(["symbol", "windowEnd"])
    want = pl.DataFrame(EXPECTED["windows"]).sort(["symbol", "windowEnd"])
    exact = ["symbol", "secType", "windowEnd", "close", "closeSourceFile", "closeSourceLine", "priceEvents"]
    assert got.select(exact).to_dicts() == want.select(exact).to_dicts()
    for col in ("ema38", "ema100"):
        assert (got[col] - want[col]).abs().max() <= EMA_TOL


def test_advisories(default_run):
    _, out = default_run
    got = pl.read_parquet(out / "advisories.parquet").sort(["symbol", "windowEnd"])
    want = pl.DataFrame(EXPECTED["advisories"]).sort(["symbol", "windowEnd"])
    ident = ["symbol", "secType", "windowEnd", "type"]
    assert got.select(ident).to_dicts() == want.select(ident).to_dicts()
    for col in ("ema38", "ema100"):
        assert (got[col] - want[col]).abs().max() <= EMA_TOL


def test_input_counts(default_run):
    manifest, out = default_run
    for name, want in EXPECTED["counts"].items():
        got = manifest["counts"]["perFile"][name]
        assert {k: got[k] for k in want} == want, name
    table = pl.read_parquet(out / "input_counts.parquet")
    assert table["accepted"].sum() == sum(c["accepted"] for c in EXPECTED["counts"].values())


def test_admission_bounds(default_run):
    manifest, _ = default_run
    bounds = EXPECTED["minBoundMs"]
    adm = manifest["admission"]
    assert adm["eventsWithPositiveMinBound"] == len(bounds)
    assert adm["smallestBoundWithNoLateEventsMs"] == max(bounds.values())
    assert adm["lateEventsByBoundMs"]["0"] == 2 and adm["lateEventsByBoundMs"]["1"] == 1
    assert manifest["counts"]["total"]["lateRejected"] == 0


def _close(out: Path, symbol: str, window_end: int) -> dict:
    df = pl.read_parquet(out / "ema_results.parquet")
    return df.filter((pl.col("symbol") == symbol) & (pl.col("windowEnd") == window_end)).to_dicts()[0]


def _check_closes(out: Path, closes: list) -> None:
    for symbol, window_end, close, src_file, src_line in closes:
        rec = _close(out, symbol, window_end)
        assert (rec["close"], rec["closeSourceFile"], rec["closeSourceLine"]) == (close, src_file, src_line)


def test_watermark_bound_zero_rejects_both_late_events(tmp_path):
    variant = EXPECTED["variants"]["watermarkBound0"]
    manifest, out = oracle(tmp_path, watermark_bound_ms=0)
    rejected = pl.read_parquet(out / "rejected_events.parquet").sort(["sourceFile", "sourceLine"])
    assert rejected.select("sourceFile", "sourceLine").rows() == [tuple(r) for r in variant["rejected"]]
    assert set(rejected["rejection"]) == {"late"}
    for rec in rejected.iter_rows(named=True):
        assert rec["minBoundMs"] == EXPECTED["minBoundMs"][f"{rec['sourceFile']}:{rec['sourceLine']}"]
    assert manifest["counts"]["perFile"][DAY1]["lateRejected"] == 1
    assert manifest["counts"]["perFile"][DAY2]["lateRejected"] == 1
    _check_closes(out, variant["closes"])


def test_watermark_bound_one_admits_the_one_ms_straggler(tmp_path):
    _, out = oracle(tmp_path, watermark_bound_ms=1)
    rejected = pl.read_parquet(out / "rejected_events.parquet")
    assert rejected.select("sourceFile", "sourceLine").rows() == [tuple(r) for r in EXPECTED["variants"]["watermarkBound1"]["rejected"]]


def test_exclusion_list(tmp_path):
    excl = tmp_path / "late.csv"
    pl.DataFrame({"sourceFile": [DAY2], "sourceLine": [63]}).write_csv(excl)
    manifest, out = oracle(tmp_path, exclude_events=excl)
    assert manifest["counts"]["perFile"][DAY2]["excluded"] == 1
    assert manifest["counts"]["perFile"][DAY2]["admitted"] == EXPECTED["counts"][DAY2]["accepted"] - 1
    rejected = pl.read_parquet(out / "rejected_events.parquet")
    assert rejected.select("sourceFile", "sourceLine", "rejection").rows() == [(DAY2, 63, "excluded")]
    day1_close = EXPECTED["variants"]["watermarkBound0"]["closes"][1]
    _check_closes(out, [day1_close])


def test_missing_trading_date_drop(tmp_path):
    variant = EXPECTED["variants"]["missingDateDrop"]
    manifest, out = oracle(tmp_path, missing_trading_date="drop")
    for name, changes in variant["countChanges"].items():
        got = manifest["counts"]["perFile"][name]
        assert {k: got[k] for k in changes} == changes
    _check_closes(out, variant["closes"])


def test_zone_offset_shifts_window_ends_only(default_run, tmp_path):
    _, base_out = default_run
    _, out = oracle(tmp_path, zone_offset="+01:00")
    base = pl.read_parquet(base_out / "ema_results.parquet")
    shifted = pl.read_parquet(out / "ema_results.parquet")
    assert shifted.with_columns(pl.col("windowEnd") + 3_600_000, pl.col("closeEventTime") + 3_600_000).equals(base)


def test_outputs_are_deterministic(default_run, tmp_path):
    first, _ = default_run
    second, _ = oracle(tmp_path)
    assert first["outputs"] == second["outputs"]


def test_crossover_summary(default_run):
    _, out = default_run
    summary = pl.read_parquet(out / "crossover_summary.parquet")
    totals = dict(summary.group_by("type").agg(pl.col("advisories").sum()).iter_rows())
    assert totals == {"BUY": 10, "SELL": 1}
    swing = summary.filter((pl.col("exchange") == "FR") & (pl.col("secType") == "I"))
    assert swing.select(pl.col("tradingDay").cast(pl.String), "type", "advisories").sort("type").rows() == [
        ("2021-11-09", "BUY", 2),
        ("2021-11-09", "SELL", 1),
    ]


def test_precision_report(default_run):
    manifest, _ = default_run
    p = manifest["precision"]
    assert p["advisory_disagreements"] == 0
    assert p["windows_over_tolerance"] == 0
    assert p["max_abs_error_ema38"] <= EMA_TOL and p["max_abs_error_ema100"] <= EMA_TOL
