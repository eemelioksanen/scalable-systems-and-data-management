"""Write the query-semantics fixtures: two synthetic day files and their expected results.

Everything expected here is stated by hand: which rows are price events, which window each
trade falls in, which trade closes each window, and which windows advise BUY or SELL.
`derivation.md` gives the reasoning row by row. The script only does the arithmetic,
evaluating the EMA recurrence in exact rational numbers (fractions.Fraction), and asserts
that the hand-stated advisories are what the spec's inequalities give on those values.
Nothing here imports or mirrors the oracle or the Flink job.

Run from the repository root:  python3 fixtures/semantics/build.py
"""

from __future__ import annotations

import datetime as dt
import json
from fractions import Fraction
from pathlib import Path

HERE = Path(__file__).resolve().parent
DAY1, DAY2 = "trading-day-08-11-21.csv", "trading-day-09-11-21.csv"
DATE1, DATE2 = "08-11-2021", "09-11-2021"

HEADER = (
    "ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,"
    "Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,"
    "Open,Nominal value,Last,Last volume,Trading time,Total volume,Mid price,Trading date,Profit,"
    "Current price,Related indices,Day high bid time, Day low bid time,Open Time, Last trade time,"
    " Close Time, Day high Time, Day low Time,Bid time,Auction Time"
)
PREAMBLE = [
    "# Synthetic query-semantics fixture. Not DEBS 2022 data.",
    "#",
    "# Same layout as the DEBS 2022 trading files: 11 comment lines with CRLF endings,",
    "# a 39-name header, one comment line after it, then LF-terminated data rows that",
    "# carry a trailing delimiter (40 fields).",
    "#",
    "# Expected results: expected.json. Reasoning: derivation.md.",
    "#",
    "# Regenerate with: python3 fixtures/semantics/build.py",
    "#",
    "# Licence: same as this repository.",
]
AFTER_HEADER = "#'Unique ID','Security Type (E)quity/(I)ndex','System date','System time','...'"


def row(symbol, sec, date, *, last="", ttime="", tdate=None, ask="", bid="", n_fields=40, tail=""):
    """One data row. Price rows default Trading date to the file's date."""
    f = [""] * 39
    f[0], f[1], f[2] = symbol, sec, date
    f[3] = ttime[:8] + ".000" if ttime else "00:00:00.000"  # system time: irrelevant to the query
    f[4], f[6] = ask, bid
    f[21], f[23] = last, ttime
    f[26] = (date if last else "") if tdate is None else tdate
    fields = f + [tail]
    return ",".join(fields[:n_fields])


def price(symbol, sec, date, ttime, last, **kw):
    return row(symbol, sec, date, last=f"{last:.6f}" if not isinstance(last, str) else last, ttime=ttime, **kw)


# ---------------------------------------------------------------------------------------
# Day 1: Monday 08-11-2021. Data rows start on line 14; the comment says which line each is.
# ---------------------------------------------------------------------------------------
day1 = [
    price("MULTI.FR", "E", DATE1, "09:00:10.000", 10),        # 14
    price("MULTI.FR", "E", DATE1, "09:04:59.999", 12),        # 15  latest in window -> close
    price("MULTI.FR", "E", DATE1, "09:03:00.000", 11),        # 16  arrives last, earlier time
    price("EDGE.NL", "E", DATE1, "10:04:59.000", 20),         # 17
    price("EDGE.NL", "E", DATE1, "10:05:00.000", 30),         # 18  exactly on a boundary: next window
    price("EDGE.NL", "E", DATE1, "10:04:59.999", 21),         # 19  behind 18 by 1 ms: close of 10:05
    price("EDGE.NL", "E", DATE1, "10:09:59.999", 31),         # 20
    price("EDGE.NL", "E", DATE1, "10:10:00.000", 40),         # 21
    price("TIE.ETR", "I", DATE1, "11:00:00.1239", 7),         # 22  sub-ms digits truncate to .123
    price("TIE.ETR", "I", DATE1, "11:00:00.123", 5),          # 23
    price("TIE.ETR", "I", DATE1, "11:00:00.123", 6),          # 24  last of the .123 tie -> close
    price("TIE.ETR", "I", DATE1, "11:00:00.122", 9),          # 25
    row("MISS.FR", "E", DATE1, ask="49.500000", bid="49.400000", ttime=""),  # 26 quote
    row("MISS.FR", "E", DATE1),                                # 27 empty payload
    price("MISS.FR", "E", DATE1, "12:00:00.100", 0),          # 28  Last = 0
    price("MISS.FR", "E", DATE1, "12:00:00.200", -1.5),       # 29  Last < 0
    price("MISS.FR", "E", DATE1, "", 50),                     # 30  no Trading time
    price("MISS.FR", "E", DATE1, "12:00:03.000", 51, tdate=""),  # 31  no Trading date: Date used
    price("MISS.FR", "E", "", "12:00:04.000", 52, tdate=""),  # 32  neither date
    price("MISS.FR", "E", DATE1, "12:00:02.000", 53),         # 33
    price("MISS.FR", "E", DATE1, "12:00:01.000", "abc"),      # 34  unparseable Last
    price("MISS.FR", "E", DATE1, "12:00:00.500", 54, n_fields=39),  # 35  no trailing delimiter
    price("MISS.FR", "E", DATE1, "12:00:00.600", 57, n_fields=38),  # 36  38 fields
    price("MISS.FR", "E", DATE1, "12:00:00.700", 58, tail="X"),     # 37  non-empty 40th field
    price("MISS.FR", "E", DATE1, "12:00:61.000", 55),         # 38  invalid seconds
    price("MISS.FR", "E", DATE1, "12:00:01.000", 56, tdate="31-02-2021"),  # 39  invalid date
    "",                                                        # 40  blank line
    price("SAME.FR", "E", DATE1, "13:01:00.000", 100),        # 41
    price("SAME.NL", "E", DATE1, "13:02:00.000", 1),          # 42  same base ID, other exchange
    price("SPARSE.ETR", "E", DATE1, "14:04:00.000", 8),       # 43
    price("SPARSE.ETR", "E", DATE1, "14:09:00.000", 9),       # 44
    price("SPARSE.ETR", "E", DATE1, "14:27:00.000", 10),      # 45  after three empty windows
    price("DAY.NL", "I", DATE1, "17:25:00.000", 40),          # 46
    "# a comment line in the middle of the data",             # 47
]

# ---------------------------------------------------------------------------------------
# Day 2: Tuesday 09-11-2021.
# ---------------------------------------------------------------------------------------
SWING_CLOSES = [100] + [1] * 46 + [100]  # windows 1..48


def swing_time(n: int) -> str:
    t = dt.datetime(2021, 11, 9, 13, 2, 30) + dt.timedelta(minutes=5 * (n - 1))
    return t.strftime("%H:%M:%S.000")


day2 = [price("DAY.NL", "I", DATE2, "09:00:00.000", 42)]  # 14
day2 += [price("SWING.FR", "I", DATE2, swing_time(n), c) for n, c in enumerate(SWING_CLOSES, 1)]  # 15-62
day2 += [price("DAY.NL", "I", DATE2, "17:26:00.000", 41, tdate=DATE1)]  # 63  yesterday's trade, today's file

FIRST_DATA_LINE = len(PREAMBLE) + 3  # preamble, header, comment after header


def write_day(name: str, rows: list[str]) -> None:
    with (HERE / name).open("w", newline="") as f:
        f.write("".join(line + "\r\n" for line in PREAMBLE))
        f.write(HEADER + "\n" + AFTER_HEADER + "\n")
        f.write("".join(r + "\n" for r in rows))


# ---------------------------------------------------------------------------------------
# Expected results, stated by hand. Window ends are clock times (exclusive end, UTC with
# --zone-offset Z); a window's close is identified by (file, line).
# ---------------------------------------------------------------------------------------
def epoch_ms(date: str, clock: str) -> int:
    d = dt.datetime.strptime(f"{date} {clock}", "%d-%m-%Y %H:%M").replace(tzinfo=dt.UTC)
    return int(d.timestamp() * 1000)


# (symbol, secType, window end date, window end clock, close, close file, close line, price events)
WINDOWS = [
    ("MULTI.FR", "E", DATE1, "09:05", 12, DAY1, 15, 3),
    ("EDGE.NL", "E", DATE1, "10:05", 21, DAY1, 19, 2),
    ("EDGE.NL", "E", DATE1, "10:10", 31, DAY1, 20, 2),
    ("EDGE.NL", "E", DATE1, "10:15", 40, DAY1, 21, 1),
    ("TIE.ETR", "I", DATE1, "11:05", 6, DAY1, 24, 4),
    ("MISS.FR", "E", DATE1, "12:05", 51, DAY1, 31, 3),
    ("SAME.FR", "E", DATE1, "13:05", 100, DAY1, 41, 1),
    ("SAME.NL", "E", DATE1, "13:05", 1, DAY1, 42, 1),
    ("SPARSE.ETR", "E", DATE1, "14:05", 8, DAY1, 43, 1),
    ("SPARSE.ETR", "E", DATE1, "14:10", 9, DAY1, 44, 1),
    ("SPARSE.ETR", "E", DATE1, "14:30", 10, DAY1, 45, 1),
    ("DAY.NL", "I", DATE1, "17:30", 41, DAY2, 63, 2),
    ("DAY.NL", "I", DATE2, "09:05", 42, DAY2, 14, 1),
] + [
    ("SWING.FR", "I", DATE2, (dt.datetime(2021, 11, 9, 13, 5) + dt.timedelta(minutes=5 * (n - 1))).strftime("%H:%M"),
     c, DAY2, 14 + n, 1)
    for n, c in enumerate(SWING_CLOSES, 1)
]

# Every symbol's first window advises BUY (EMA_0 = 0, D-7). SWING.FR then crosses below at
# its window 46 (derivation.md) and back above at window 48, where the close returns to 100.
ADVISORIES = [
    ("MULTI.FR", "BUY", DATE1, "09:05"),
    ("EDGE.NL", "BUY", DATE1, "10:05"),
    ("TIE.ETR", "BUY", DATE1, "11:05"),
    ("MISS.FR", "BUY", DATE1, "12:05"),
    ("SAME.FR", "BUY", DATE1, "13:05"),
    ("SAME.NL", "BUY", DATE1, "13:05"),
    ("SPARSE.ETR", "BUY", DATE1, "14:05"),
    ("DAY.NL", "BUY", DATE1, "17:30"),
    ("SWING.FR", "BUY", DATE2, "13:05"),
    ("SWING.FR", "SELL", DATE2, "16:50"),  # window 46
    ("SWING.FR", "BUY", DATE2, "17:00"),  # window 48
]

COUNTS = {
    DAY1: {"lines": 47, "comment": 13, "header": 1, "blank": 1, "malformedRow": 2, "nonPrice": 2,
           "malformedPrice": 1, "nonPositivePrice": 2, "missingTimestamp": 2, "malformedTimestamp": 2,
           "accepted": 21, "systemDateEvents": 1, "staleTradingDateEvents": 0},
    DAY2: {"lines": 63, "comment": 12, "header": 1, "blank": 0, "malformedRow": 0, "nonPrice": 0,
           "malformedPrice": 0, "nonPositivePrice": 0, "missingTimestamp": 0, "malformedTimestamp": 0,
           "accepted": 50, "systemDateEvents": 0, "staleTradingDateEvents": 1},
}

# Source-order admission (README "Late events"): only two events arrive after the maximum
# event time seen so far has already reached their window's end.
#  - day 1 line 19 (10:04:59.999, window 10:05) follows line 18 at 10:05:00.000: bound 1 ms.
#  - day 2 line 63 (day 1 17:26, window day 1 17:30) follows SWING.FR's last trade at
#    day 2 16:57:30: bound = day2 16:57:30 - day1 17:30 + 1 ms.
MIN_BOUNDS = {
    f"{DAY1}:19": 1,
    f"{DAY2}:63": (epoch_ms(DATE2, "16:57") + 30_000) - epoch_ms(DATE1, "17:30") + 1,
}

# Variants: what changes, stated by hand.
VARIANTS = {
    # --watermark-bound-ms 0 rejects both events above; their windows fall back to the
    # next-latest trade.
    "watermarkBound0": {
        "rejected": [[DAY1, 19], [DAY2, 63]],
        "closes": [["EDGE.NL", epoch_ms(DATE1, "10:05"), 20, DAY1, 17], ["DAY.NL", epoch_ms(DATE1, "17:30"), 40, DAY1, 46]],
    },
    # --watermark-bound-ms 1 admits line 19 (bound exactly 1) but not line 63.
    "watermarkBound1": {"rejected": [[DAY2, 63]]},
    # --missing-trading-date drop: line 31 loses its date, so line 33 closes MISS.FR.
    "missingDateDrop": {
        "closes": [["MISS.FR", epoch_ms(DATE1, "12:05"), 53, DAY1, 33]],
        "countChanges": {DAY1: {"accepted": 20, "missingTimestamp": 3, "systemDateEvents": 0}},
    },
}


# --empty-windows carry-forward (D-42): every gap between two windows of a symbol is filled with
# windows that repeat the earlier window's close and hold no price event. Only two symbols have
# gaps: SPARSE.ETR's three empty windows, and DAY.NL's night from day 1 17:30 to day 2 09:05.
# (symbol, secType, first filled window end, last filled window end, close, close file, close line)
CARRY_FORWARD_GAPS = [
    ("SPARSE.ETR", "E", (DATE1, "14:15"), (DATE1, "14:25"), 9, DAY1, 44),
    ("DAY.NL", "I", (DATE1, "17:35"), (DATE2, "09:00"), 41, DAY2, 63),
]
# Both gaps repeat a close at or above every earlier one, so EMA38 stays above EMA100 and the
# advisories are the same as when empty windows are skipped.
CARRY_FORWARD_ADVISORIES = ADVISORIES


def window_clock(epoch: int) -> tuple[str, str]:
    t = dt.datetime.fromtimestamp(epoch / 1000, dt.UTC)
    return t.strftime("%d-%m-%Y"), t.strftime("%H:%M")


def carried_windows():
    filled = []
    for sym, sec, first, last, close, src_file, src_line in CARRY_FORWARD_GAPS:
        for end in range(epoch_ms(*first), epoch_ms(*last) + 1, 300_000):
            filled.append((sym, sec, *window_clock(end), close, src_file, src_line, 0))
    return WINDOWS + filled


def ema_sequences(windows=WINDOWS):
    """Exact EMAs per window, in rational arithmetic, from the hand-stated closes."""
    a38, a100 = Fraction(2, 39), Fraction(2, 101)
    state: dict[str, tuple[Fraction, Fraction]] = {}
    out = []
    for sym, sec, date, clock, close, src_file, src_line, n in sorted(windows, key=lambda w: (w[0], epoch_ms(w[2], w[3]))):
        p38, p100 = state.get(sym, (Fraction(0), Fraction(0)))
        c = Fraction(close)
        e38 = c * a38 + p38 * (1 - a38)
        e100 = c * a100 + p100 * (1 - a100)
        state[sym] = (e38, e100)
        kind = "BUY" if e38 > e100 and p38 <= p100 else "SELL" if e38 < e100 and p38 >= p100 else None
        out.append(((sym, sec, epoch_ms(date, clock), float(c), float(e38), float(e100), src_file, src_line, n), kind))
    return out


def window_record(w) -> dict:
    return dict(zip(("symbol", "secType", "windowEnd", "close", "ema38", "ema100", "closeSourceFile", "closeSourceLine", "priceEvents"), w))


def advisory_record(w, kind) -> dict:
    return {"symbol": w[0], "secType": w[1], "windowEnd": w[2], "type": kind, "ema38": w[4], "ema100": w[5]}


def main() -> None:
    write_day(DAY1, day1)
    write_day(DAY2, day2)

    # The line numbers in the comments above are part of the contract; check them.
    assert FIRST_DATA_LINE == 14
    assert day1[19 - FIRST_DATA_LINE].startswith("EDGE.NL") and ",21.000000," in day1[19 - FIRST_DATA_LINE]
    assert day1[47 - FIRST_DATA_LINE].startswith("#")
    assert day2[63 - FIRST_DATA_LINE].startswith("DAY.NL") and day2[63 - FIRST_DATA_LINE].count(DATE1) == 1
    assert day2[62 - FIRST_DATA_LINE].startswith("SWING.FR") and swing_time(48) == "16:57:30.000"
    for name, rows in ((DAY1, day1), (DAY2, day2)):
        assert COUNTS[name]["lines"] == FIRST_DATA_LINE - 1 + len(rows)

    seq = ema_sequences()
    computed_advisories = sorted((w[0], k, w[2]) for w, k in seq if k)
    stated_advisories = sorted((s, k, epoch_ms(d, c)) for s, k, d, c in ADVISORIES)
    assert computed_advisories == stated_advisories, (computed_advisories, stated_advisories)

    carried = ema_sequences(carried_windows())
    ends: dict[str, list[int]] = {}
    for w, _ in carried:
        ends.setdefault(w[0], []).append(w[2])
    assert all(b - a == 300_000 for e in ends.values() for a, b in zip(e, e[1:])), "a gap is left unfilled"
    assert len(carried) == len(seq) + 3 + 186
    carried_advisories = sorted((w[0], k, w[2]) for w, k in carried if k)
    stated_carried = sorted((s, k, epoch_ms(d, c)) for s, k, d, c in CARRY_FORWARD_ADVISORIES)
    assert carried_advisories == stated_carried, (carried_advisories, stated_carried)
    VARIANTS["emptyWindowsCarryForward"] = {
        "windows": [window_record(w) for w, _ in carried],
        "advisories": [advisory_record(w, k) for w, k in carried if k],
    }

    expected = {
        "description": "Hand-derived expectations for the synthetic day files; see derivation.md.",
        "zoneOffset": "Z",
        "counts": COUNTS,
        "windows": [window_record(w) for w, _ in seq],
        "advisories": [advisory_record(w, k) for w, k in seq if k],
        "minBoundMs": MIN_BOUNDS,
        "variants": VARIANTS,
    }
    (HERE / "expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(f"wrote {DAY1}, {DAY2}, expected.json: {len(expected['windows'])} windows, {len(expected['advisories'])} advisories")


if __name__ == "__main__":
    main()
