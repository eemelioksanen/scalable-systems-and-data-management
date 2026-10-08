"""The oracle reproduces the hand-worked sequences in fixtures/golden/ (derivation.md there).

Each golden window becomes one trade at the window's last millisecond, written in the real
file layout, so the sequences go through the same parsing and windowing as real input.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest

from ema_oracle.run import Config, run

GOLDEN = Path(__file__).resolve().parents[2] / "fixtures" / "golden"
TOLERANCE = 1e-9  # D-18


def _as_csv(fixture: dict, directory: Path) -> Path:
    first = dt.datetime.fromtimestamp(fixture["windows"][0]["windowEnd"] / 1000 - 1, dt.UTC)
    path = directory / f"golden-{first:%d-%m-%y}.csv"
    lines = ["# golden fixture", "ID,SecType,Date,Time,..."]
    for w in fixture["windows"]:
        t = dt.datetime.fromtimestamp((w["windowEnd"] - 1) / 1000, dt.UTC)
        f = [""] * 40
        f[0], f[1], f[2], f[3] = fixture["symbol"], fixture["secType"], f"{t:%d-%m-%Y}", f"{t:%H:%M:%S}.000"
        f[21], f[23] = f"{w['close']:.6f}", f"{t:%H:%M:%S}.{t.microsecond // 1000:03d}"
        lines.append(",".join(f))
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.mark.parametrize("name", ["synthetic", "real"])
def test_golden_sequence(name, tmp_path):
    fixture = json.loads((GOLDEN / f"{name}.json").read_text())
    csv = _as_csv(fixture, tmp_path)
    # _as_csv writes the window times in UTC.
    run(Config(inputs=[csv], out=tmp_path / "out", zone_offset="Z", input_checksums=False))

    got = pl.read_parquet(tmp_path / "out" / "ema_results.parquet")
    want = pl.DataFrame(fixture["windows"])
    assert got["windowEnd"].to_list() == want["windowEnd"].to_list()
    assert got["close"].to_list() == want["close"].to_list()
    for col in ("ema38", "ema100"):
        assert (got[col] - want[col]).abs().max() <= TOLERANCE

    adv = pl.read_parquet(tmp_path / "out" / "advisories.parquet")
    assert adv.select("symbol", "secType", "windowEnd", "type").to_dicts() == [
        {k: a[k] for k in ("symbol", "secType", "windowEnd", "type")} for a in fixture["advisories"]
    ]
