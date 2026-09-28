"""Derives the hand-worked EMA/advisory sequences and writes them as fixtures.

Two sequences, per the ticket:

  synthetic  - closes chosen so every branch is exercised: the BUY forced by the
               zero-initialised EMAs on the first window, a stretch that stays
               bullish without repeating the advisory, and a genuine SELL.
  real       - the first windows of the busiest symbol in the sampled slice,
               so the same arithmetic is shown against data that actually exists.

The values here are computed, but each one is reproduced by hand in
`fixtures/golden/derivation.md`; this script is what keeps the two in step.
"""
from __future__ import annotations

import json, os, sys
from pathlib import Path

OUT = Path(os.environ.get("FIXTURES_DIR") or ("/fixtures" if Path("/fixtures").exists() else "fixtures"))
GOLDEN = OUT / "golden"

WINDOW_MS = 5 * 60 * 1000


def alpha(j: int) -> float:
    """Smoothing factor of the spec: 2 / (1 + j)."""
    return 2.0 / (1.0 + j)


def ema(close: float, prev: float, j: int) -> float:
    a = alpha(j)
    return close * a + prev * (1.0 - a)


def advisory(prev38: float, prev100: float, cur38: float, cur100: float) -> str | None:
    """The spec's crossover inequalities, evaluated on consecutive emitted windows.

    BUY  : EMA38 was at or below EMA100, and is now strictly above.
    SELL : EMA38 was at or above EMA100, and is now strictly below.
    Equality on the previous window counts as 'not yet crossed', which is what makes
    the zero-initialised first window emit a BUY for any positive close.
    """
    if prev38 <= prev100 and cur38 > cur100:
        return "BUY"
    if prev38 >= prev100 and cur38 < cur100:
        return "SELL"
    return None


def derive(symbol: str, sec_type: str, closes: list[tuple[int, float]]) -> dict:
    """Runs the spec semantics over (window_end, close) pairs that each had >= 1 trade."""
    e38 = e100 = 0.0
    windows, advisories = [], []
    for window_end, close in closes:
        p38, p100 = e38, e100
        e38, e100 = ema(close, p38, 38), ema(close, p100, 100)
        windows.append({
            "windowEnd": window_end, "close": close,
            "ema38": e38, "ema100": e100,
            "prevEma38": p38, "prevEma100": p100,
        })
        if (kind := advisory(p38, p100, e38, e100)) is not None:
            advisories.append({
                "symbol": symbol, "secType": sec_type,
                "windowEnd": window_end, "type": kind,
                "ema38": e38, "ema100": e100,
            })
    return {
        "symbol": symbol, "secType": sec_type,
        "alpha38": alpha(38), "alpha100": alpha(100),
        "initialEma": 0.0, "windowMs": WINDOW_MS,
        "windows": windows, "advisories": advisories,
    }


def synthetic() -> dict:
    """Closes picked to force every branch, independently of what the slice contains.

    A constant close is used on purpose: with a fixed close the recurrence collapses to
    EMA_n = close * (1 - (1-a)^n), so each value in `derivation.md` can be checked by
    hand without carrying rounding through fifty steps.

    Phase 1 (24 windows at 100.00): both EMAs start at 0, so the very first window emits
    BUY, and EMA38 - rising faster - stays above EMA100 for the rest of the phase. That
    is the required interval with no repeated advisory.

    Phase 2 (26 windows at 5.00): both EMAs decay toward the new level, EMA38 faster,
    so it eventually crosses back below EMA100 and emits the SELL. The crossover is slow
    by construction - roughly 21 windows - because that is genuinely how far apart the
    two smoothing factors drive the averages, not a number chosen for convenience.
    """
    base = 1_636_329_600_000  # 2021-11-08T00:00:00Z, itself a 5-minute boundary
    closes = [100.0] * 24 + [5.0] * 26
    return derive("SYNTH.TEST", "E",
                  [(base + (i + 1) * WINDOW_MS, c) for i, c in enumerate(closes)])


def real(top_symbol: str | None) -> dict | None:
    """First windows of the busiest symbol in the slice, using the same code path."""
    import duckdb

    data = Path(os.environ.get("DATA_DIR") or ("/data" if Path("/data").exists() else "data"))
    parsed = data / "slice" / "parsed.parquet"
    if not parsed.exists():
        return None

    con = duckdb.connect()
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{parsed}')")
    # `Trading date` is absent on most price rows in this feed, so the date part falls
    # back to the system `Date`. Trading time is always present, and both columns agree
    # wherever Trading date does appear. This fallback is what D-10 leaves open pending
    # the fuller EDA; it is stated here so the fixture is not silently built on the
    # unrepresentative minority of rows that happen to carry a trading date.
    con.execute("""
        CREATE VIEW p AS
        SELECT id, sec_type, chunk, src_pos,
               TRY_CAST(last AS DOUBLE) AS price,
               strptime(CASE WHEN trading_date <> '' THEN trading_date ELSE date END
                        || ' ' || trading_time, '%d-%m-%Y %H:%M:%S.%g') AS ts
        FROM t
        WHERE last <> '' AND TRY_CAST(last AS DOUBLE) > 0 AND trading_time <> ''
    """)
    if top_symbol is None:
        row = con.sql("SELECT id FROM p GROUP BY 1 ORDER BY count(*) DESC LIMIT 1").fetchone()
        if row is None:
            return None
        top_symbol = row[0]

    # The window close is the trade with the latest event time; ties break on source
    # position, which is why pass 1 of the profiler records it.
    rows = con.execute("""
        WITH w AS (
          SELECT *, epoch_ms(ts) - (epoch_ms(ts) % ?) + ? AS window_end FROM p WHERE id = ?
        ), ranked AS (
          SELECT *, row_number() OVER (
            PARTITION BY window_end ORDER BY ts DESC, chunk DESC, src_pos DESC) AS rn
          FROM w
        )
        SELECT window_end, price, sec_type FROM ranked WHERE rn = 1
        ORDER BY window_end LIMIT 16
    """, [WINDOW_MS, WINDOW_MS, top_symbol]).fetchall()
    if not rows:
        return None
    out = derive(top_symbol, rows[0][2], [(int(r[0]), float(r[1])) for r in rows])
    out["note"] = ("Windows with no trade are absent by construction: this lists only windows "
                   "that had at least one price event, which is the D-6 empty-window policy.")
    return out


def main() -> int:
    GOLDEN.mkdir(parents=True, exist_ok=True)
    syn = synthetic()

    kinds = [a["type"] for a in syn["advisories"]]
    assert kinds[0] == "BUY", "zero-initialised EMAs must emit BUY on the first positive close"
    assert "SELL" in kinds, "synthetic sequence must contain a SELL"
    assert len(syn["windows"]) > len(syn["advisories"]), "must contain windows with no advisory"
    (GOLDEN / "synthetic.json").write_text(json.dumps(syn, indent=2) + "\n")
    print(f"synthetic: {len(syn['windows'])} windows, advisories {kinds}")

    rl = real(os.environ.get("GOLDEN_SYMBOL"))
    if rl is None:
        print("real fixture skipped: no parsed slice available", file=sys.stderr)
        return 0
    (GOLDEN / "real.json").write_text(json.dumps(rl, indent=2) + "\n")
    print(f"real: {rl['symbol']}, {len(rl['windows'])} windows, "
          f"advisories {[a['type'] for a in rl['advisories']]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
