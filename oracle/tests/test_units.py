from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from ema_oracle.ema import crossover
from ema_oracle.parse import file_date, order_inputs, parse_zone_offset


@pytest.mark.parametrize(
    ("prev", "cur", "want"),
    [
        ((0.0, 0.0), (5.0, 2.0), "BUY"),  # first positive close from the zero start (D-7)
        ((1.0, 1.0), (2.0, 1.0), "BUY"),  # previous equality allows BUY
        ((1.0, 1.0), (1.0, 2.0), "SELL"),  # ... and SELL
        ((1.0, 2.0), (2.0, 1.0), "BUY"),
        ((2.0, 1.0), (1.0, 2.0), "SELL"),
        ((2.0, 1.0), (3.0, 1.0), None),  # stays above: no repeat
        ((1.0, 2.0), (1.0, 3.0), None),  # stays below: no repeat
        ((1.0, 2.0), (2.0, 2.0), None),  # current tie is neither
        ((2.0, 1.0), (2.0, 2.0), None),
        ((1.0, 1.0), (1.0, 1.0), None),
    ],
)
def test_crossover_inequalities(prev, cur, want):
    assert crossover(*prev, *cur) == want


def test_crossover_after_a_tie_uses_the_tie_as_previous():
    # A window that ends exactly tied advises nothing, and the next window may go either way.
    assert crossover(2.0, 1.0, 2.0, 2.0) is None
    assert crossover(2.0, 2.0, 1.0, 2.0) == "SELL"
    assert crossover(2.0, 2.0, 3.0, 2.0) == "BUY"


@pytest.mark.parametrize(
    ("text", "ms"),
    [("Z", 0), ("+01:00", 3_600_000), ("+02:00", 7_200_000), ("-05:30", -19_800_000), ("+0100", 3_600_000)],
)
def test_zone_offset(text, ms):
    assert parse_zone_offset(text) == ms


@pytest.mark.parametrize("text", ["CET", "+1", "+25:00", "01:00"])
def test_zone_offset_rejects(text):
    with pytest.raises(ValueError):
        parse_zone_offset(text)


def test_inputs_are_ordered_by_file_date_not_argument_order():
    names = ["debs2022-gc-trading-day-14-11-21.csv", "debs2022-gc-trading-day-08-11-21.csv", "debs2022-gc-trading-day-12-11-21.csv"]
    ordered = [p.name for p in order_inputs([Path(n) for n in names])]
    assert ordered == sorted(names, key=lambda n: file_date(Path(n)))
    assert file_date(Path(names[0])) == dt.date(2021, 11, 14)


def test_undated_input_is_rejected():
    with pytest.raises(ValueError):
        order_inputs([Path("trades.csv")])
