"""EMAs over consecutive non-empty windows, and the crossover advisories between them.

Spec, per window with closing price c and j in {38, 100}:

    EMA_j = c * (2 / (1 + j)) + EMA_j,prev * (1 - 2 / (1 + j)),    EMA_j,0 = 0   (D-7)

A window with no price event does not exist for this purpose (D-6): the EMA is unchanged
and no crossover is checked. State carries across days and the weekend (D-8).

The EMAs are computed in IEEE 754 binary64 with the formula written exactly as above.
`precision_check` recomputes every value in 50-digit decimal arithmetic from the same
binary64 closes and reports how far the binary64 values drift from it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Context, Decimal

import polars as pl

J_SHORT = 38
J_LONG = 100
ALPHA_SHORT = 2.0 / (1 + J_SHORT)
ALPHA_LONG = 2.0 / (1 + J_LONG)

# Below this |EMA38 - EMA100| the sign of the difference is within a few ulps for the
# largest prices in the data (~1.1e6, ulp ~1.2e-10), so another correct implementation
# that orders its floating-point operations differently could disagree on the advisory.
NEAR_TIE = 1e-9

# The absolute EMA tolerance of the correctness check (D-18).
TOLERANCE = 1e-9


def crossover(prev_short: float, prev_long: float, short: float, long: float) -> str | None:
    """The spec's BUY and SELL inequalities, nothing more."""
    if short > long and prev_short <= prev_long:
        return "BUY"
    if short < long and prev_short >= prev_long:
        return "SELL"
    return None


def compute(closes: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (ema_results, advisories) for windows sorted by (symbol, windowEnd)."""
    symbols = closes["symbol"].to_list()
    prices = closes["close"].to_list()
    keep_short = 1.0 - ALPHA_SHORT
    keep_long = 1.0 - ALPHA_LONG

    ema_short = [0.0] * len(prices)
    ema_long = [0.0] * len(prices)
    adv_rows: list[int] = []
    adv_types: list[str] = []

    current = None
    s = lng = 0.0
    for i, (sym, c) in enumerate(zip(symbols, prices)):
        if sym != current:
            current, s, lng = sym, 0.0, 0.0
        prev_s, prev_l = s, lng
        s = c * ALPHA_SHORT + prev_s * keep_short
        lng = c * ALPHA_LONG + prev_l * keep_long
        ema_short[i], ema_long[i] = s, lng
        kind = crossover(prev_s, prev_l, s, lng)
        if kind is not None:
            adv_rows.append(i)
            adv_types.append(kind)

    results = closes.with_columns(
        ema38=pl.Series(ema_short, dtype=pl.Float64),
        ema100=pl.Series(ema_long, dtype=pl.Float64),
    )
    advisories = (
        results[adv_rows]
        .with_columns(type=pl.Series(adv_types, dtype=pl.String))
        .select("symbol", "secType", "windowEnd", "type", "ema38", "ema100")
    )
    return results, advisories


@dataclass
class PrecisionReport:
    windows: int
    max_abs_error_ema38: float
    max_abs_error_ema100: float
    max_error_ulps: float
    worst_symbol: str | None
    windows_over_tolerance: int
    symbols_over_tolerance: int
    advisory_disagreements: int
    near_ties: int


def precision_check(results: pl.DataFrame) -> PrecisionReport:
    """Recompute every EMA in 50-digit decimal and compare with the binary64 values.

    The closes are taken as their exact binary64 values, so this isolates the rounding the
    recurrence accumulates; `max_error_ulps` expresses it in units in the last place of the
    exact value. `windows_over_tolerance` counts windows where either EMA differs from the
    exact value by more than the D-18 tolerance. `advisory_disagreements` counts windows whose BUY/SELL decision
    differs between the two arithmetics; `near_ties` counts windows whose binary64
    |EMA38 - EMA100| is at most NEAR_TIE.
    """
    ctx = Context(prec=50)
    a_s = ctx.divide(Decimal(2), Decimal(1 + J_SHORT))
    a_l = ctx.divide(Decimal(2), Decimal(1 + J_LONG))
    k_s = ctx.subtract(Decimal(1), a_s)
    k_l = ctx.subtract(Decimal(1), a_l)
    zero = Decimal(0)

    err_s = err_l = max_ulps = worst = 0.0
    worst_symbol = None
    over = 0
    over_symbols: set[str] = set()
    disagreements = near = 0
    current = None
    s = lng = zero
    fs_prev = fl_prev = 0.0
    for sym, c, fs, fl in zip(
        results["symbol"].to_list(),
        results["close"].to_list(),
        results["ema38"].to_list(),
        results["ema100"].to_list(),
    ):
        if sym != current:
            current, s, lng = sym, zero, zero
            fs_prev = fl_prev = 0.0
        dc = Decimal(c)
        prev_s, prev_l = s, lng
        s = ctx.add(ctx.multiply(dc, a_s), ctx.multiply(prev_s, k_s))
        lng = ctx.add(ctx.multiply(dc, a_l), ctx.multiply(prev_l, k_l))
        e_s = abs(fs - float(s))
        e_l = abs(fl - float(lng))
        err_s = max(err_s, e_s)
        err_l = max(err_l, e_l)
        max_ulps = max(max_ulps, e_s / math.ulp(float(s)), e_l / math.ulp(float(lng)))
        if max(e_s, e_l) > worst:
            worst, worst_symbol = max(e_s, e_l), sym
        if e_s > TOLERANCE or e_l > TOLERANCE:
            over += 1
            over_symbols.add(sym)
        exact_kind = crossover(prev_s, prev_l, s, lng)
        float_kind = crossover(fs_prev, fl_prev, fs, fl)
        disagreements += exact_kind != float_kind
        near += abs(fs - fl) <= NEAR_TIE
        fs_prev, fl_prev = fs, fl

    return PrecisionReport(
        windows=results.height,
        max_abs_error_ema38=err_s,
        max_abs_error_ema100=err_l,
        max_error_ulps=max_ulps,
        worst_symbol=worst_symbol,
        windows_over_tolerance=over,
        symbols_over_tolerance=len(over_symbols),
        advisory_disagreements=disagreements,
        near_ties=near,
    )
