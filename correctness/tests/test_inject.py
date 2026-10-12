"""The injector, judged by the oracle's results and its admission model."""

from __future__ import annotations

import datetime as dt
import random
from collections import defaultdict
from pathlib import Path

import polars as pl
import pytest

from ema_check.inject import Options, inject, price_event
from ema_oracle.run import Config, run

SEMANTICS = Path(__file__).resolve().parents[2] / "fixtures" / "semantics"
D18 = ["symbol", "secType", "windowEnd", "close", "ema38", "ema100"]


def _row(symbol: str, t: dt.datetime | None, price: str, trading_date: bool, system_date: dt.date) -> str:
    f = [""] * 40
    f[0], f[1], f[2], f[3] = symbol, "E", f"{system_date:%d-%m-%Y}", "09:00:00.000"
    if t is not None:
        f[21], f[23] = price, f"{t:%H:%M:%S}.{t.microsecond // 1000:03d}"
        if trading_date:
            f[26] = f"{t:%d-%m-%Y}"
    return ",".join(f)


def _synthetic_day(directory: Path, day: dt.date, rng: random.Random, rows: int = 6000) -> Path:
    """A day of 30 symbols whose prices are unique, with backward jumps and equal timestamps."""
    path = directory / f"synthetic-{day:%d-%m-%y}.csv"
    lines = ["# synthetic\r", "ID,SecType,Date,Time,Ask", "# after the header"]
    clock = dt.datetime.combine(day, dt.time(9))
    for n in range(rows):
        symbol = f"S{rng.randrange(30)}.FR"
        if rng.random() < 0.4:
            lines.append(_row(symbol, None, "", False, day))
            continue
        clock += dt.timedelta(milliseconds=rng.choice([0, 0, 1, 50, 900, 4000]))
        t = clock - dt.timedelta(milliseconds=rng.choice([0, 0, 0, 30, 2000, 400_000]))
        lines.append(_row(symbol, t, f"{10 + n / 1000:.6f}", rng.random() < 0.3, day))
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture(scope="module")
def week(tmp_path_factory) -> list[Path]:
    directory = tmp_path_factory.mktemp("input")
    rng = random.Random(7)
    return [_synthetic_day(directory, dt.date(2021, 11, d), rng) for d in (8, 9)]


def _oracle(inputs: list[Path], out: Path, **kw) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    manifest = run(Config(inputs=inputs, out=out, input_checksums=False, precision_check=False, **kw))
    return manifest, pl.read_parquet(out / "ema_results.parquet").select(D18), pl.read_parquet(out / "advisories.parquet").select(D18[:3] + ["type"])


def _event_order(paths: list[Path]) -> list:
    return [e for p in paths for line in p.read_bytes().split(b"\n") if (e := price_event(line))]


def test_disorder_keeps_every_row_and_is_deterministic(week, tmp_path):
    a = inject(week, tmp_path / "a", Options(disorder_ms=5000, seed=1))
    b = inject(week, tmp_path / "b", Options(disorder_ms=5000, seed=1))
    c = inject(week, tmp_path / "c", Options(disorder_ms=5000, seed=2))
    assert [f["outputMd5"] for f in a["files"]] == [f["outputMd5"] for f in b["files"]]
    assert [f["outputMd5"] for f in a["files"]] != [f["outputMd5"] for f in c["files"]]
    for src in week:
        got = (tmp_path / "a" / src.name).read_bytes().split(b"\n")
        want = src.read_bytes().split(b"\n")
        assert sorted(got) == sorted(want)
        assert got != want


def test_disorder_preserves_equal_timestamp_order(week, tmp_path):
    inject(week, tmp_path / "out", Options(disorder_ms=60_000, seed=3))
    for src in week:
        def groups(path: Path) -> dict:
            order = defaultdict(list)
            for line in path.read_bytes().split(b"\n"):
                if event := price_event(line):
                    order[event].append(line.split(b",")[21])
            return order
        want = groups(src)
        assert any(len(v) > 1 for v in want.values())
        assert groups(tmp_path / "out" / src.name) == want


@pytest.mark.parametrize("disorder_ms", [1000, 60_000])
def test_disorder_needs_at_most_the_bound_more(week, tmp_path, disorder_ms):
    base, base_results, base_adv = _oracle(week, tmp_path / "base", zone_offset="Z")
    inject(week, tmp_path / "in", Options(disorder_ms=disorder_ms, seed=4))
    shuffled = sorted((tmp_path / "in").glob("*.csv"))
    need = base["admission"]["smallestBoundWithNoLateEventsMs"]

    got, results, adv = _oracle(shuffled, tmp_path / "admit-all", zone_offset="Z")
    assert _event_order(shuffled) != _event_order(week)
    assert got["admission"]["smallestBoundWithNoLateEventsMs"] <= need + disorder_ms
    assert results.equals(base_results) and adv.equals(base_adv)

    bounded, results, adv = _oracle(shuffled, tmp_path / "bounded", zone_offset="Z", watermark_bound_ms=need + disorder_ms)
    assert bounded["counts"]["total"]["lateRejected"] == 0
    assert results.equals(base_results) and adv.equals(base_adv)


def test_semantics_fixtures_survive_disorder(tmp_path):
    inputs = sorted(SEMANTICS.glob("*.csv"))
    _, want, want_adv = _oracle(inputs, tmp_path / "base", zone_offset="Z")
    inject(inputs, tmp_path / "in", Options(disorder_ms=10_000, seed=5))
    _, got, got_adv = _oracle(sorted((tmp_path / "in").glob("*.csv")), tmp_path / "out", zone_offset="Z")
    assert got.equals(want) and got_adv.equals(want_adv)


def test_late_events_match_the_oracle_model(week, tmp_path):
    late_by = 120_000
    manifest = inject(week, tmp_path / "in", Options(disorder_ms=1000, late_rate=0.01, late_by_ms=late_by, seed=6))
    listed = pl.read_parquet(tmp_path / "in" / "late_events.parquet")
    assert manifest["lateEvents"] == listed.height > 10

    oracle, _, _ = _oracle(sorted((tmp_path / "in").glob("*.csv")), tmp_path / "out", zone_offset="Z", watermark_bound_ms=late_by)
    rejected = pl.read_parquet(tmp_path / "out" / "rejected_events.parquet").select("sourceFile", "sourceLine", "symbol", "minBoundMs")
    beyond = listed.filter(pl.col("minBoundMs") > late_by)
    assert beyond.join(rejected, on=["sourceFile", "sourceLine", "symbol", "minBoundMs"], how="anti").is_empty()
    assert oracle["counts"]["total"]["lateRejected"] >= beyond.height
