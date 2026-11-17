# Query-semantics fixtures, worked by hand

Two synthetic day files in the real DEBS 2022 layout, with every expected result stated by
hand. They exercise the rules the golden sequences in [`../golden/`](../golden/derivation.md)
do not reach: parsing, window assignment, close selection, ties, missing values, exchanges,
gaps, day boundaries and late events. The golden sequences cover the EMA arithmetic.

`build.py` writes the CSV files and `expected.json`. The windows, closes, advisories,
counts and admission bounds in it are typed in by hand; the script only evaluates the EMA
recurrence in exact rational arithmetic and asserts that the hand-stated advisories are
what the spec's inequalities give. It does not import or mirror the oracle or the Flink job.

All times below are `--zone-offset Z`. A window is named by its exclusive end: window
`09:05` holds trades from `09:00:00.000` to `09:04:59.999`. Lines are physical line numbers;
data starts at line 14, after 11 comment lines, the header and the comment after it.

## Day 1: `trading-day-08-11-21.csv` (Monday)

### `MULTI.FR`: several trades, out-of-order arrival (lines 14-16)

| line | Trading time | Last |
|---|---|---|
| 14 | 09:00:10.000 | 10 |
| 15 | 09:04:59.999 | 12 |
| 16 | 09:03:00.000 | 11 |

All three fall in window `09:05`. Line 16 arrives last but traded earlier than line 15, so
the close is **12 (line 15)**: latest event time, not last arrival (D-4).

### `EDGE.NL`: exact window boundaries (lines 17-21)

| line | Trading time | Last | window |
|---|---|---|---|
| 17 | 10:04:59.000 | 20 | 10:05 |
| 18 | 10:05:00.000 | 30 | **10:10**: a trade exactly on a boundary opens the next window |
| 19 | 10:04:59.999 | 21 | 10:05 |
| 20 | 10:09:59.999 | 31 | 10:10 |
| 21 | 10:10:00.000 | 40 | 10:15 |

Closes: `10:05` = **21** (line 19 is later than line 17), `10:10` = **31** (line 20 is later
than line 18), `10:15` = **40**. Prices rise throughout, so after the first-window BUY EMA38
stays above EMA100 and nothing more is advised.

Line 19 is also the fixture's 1 ms straggler: it arrives after line 18 has already reached
the end of its window (see "Late events" below).

### `TIE.ETR`: equal and sub-millisecond timestamps (lines 22-25)

| line | Trading time | Last |
|---|---|---|
| 22 | 11:00:00.1239 | 7 |
| 23 | 11:00:00.123 | 5 |
| 24 | 11:00:00.123 | 6 |
| 25 | 11:00:00.122 | 9 |

Event time is epoch milliseconds, with finer digits truncated (D-22, D-26), so line 22 is
also at `.123`. Lines 22, 23 and 24 tie; source position breaks the tie (D-23) and the last
of them wins. The close is **6 (line 24)**. Had the fourth digit been kept, 7 would have won.

### `MISS.FR`: rows that are not price events (lines 26-39)

| line | content | category |
|---|---|---|
| 26 | `Ask`/`Bid` only | nonPrice |
| 27 | empty payload | nonPrice |
| 28 | `Last` = 0.000000 | nonPositivePrice (D-2) |
| 29 | `Last` = -1.500000 | nonPositivePrice |
| 30 | `Last` = 50, no `Trading time` | missingTimestamp |
| 31 | `Last` = 51 at 12:00:03.000, no `Trading date` | **accepted**, `Date` substituted (D-25) |
| 32 | `Last` = 52, neither `Trading date` nor `Date` | missingTimestamp |
| 33 | `Last` = 53 at 12:00:02.000 | **accepted** |
| 34 | `Last` = `abc` | malformedPrice |
| 35 | `Last` = 54 at 12:00:00.500, 39 fields (no trailing delimiter) | **accepted** (D-21) |
| 36 | 38 fields | malformedRow |
| 37 | 40 fields, last one non-empty | malformedRow |
| 38 | `Trading time` 12:00:61.000 | malformedTimestamp |
| 39 | `Trading date` 31-02-2021 | malformedTimestamp |

Window `12:05` has three accepted trades, and the close is **51 (line 31)**, the latest.
With `--missing-trading-date drop`, line 31 becomes missingTimestamp and the close is **53
(line 33)**.

Line 40 is blank; line 47 is a comment in the middle of the data. Both are skipped by
content, not by position.

### `SAME.FR` / `SAME.NL`: one base ID on two exchanges (lines 41-42)

`SAME.FR` trades at 100 and `SAME.NL` at 1, both in window `13:05`. The key is the full
`ID` (D-1), so these are two symbols with their own EMAs and their own first-window BUY.
Merging them would give one window whose close is 1.

### `SPARSE.ETR`: empty windows (lines 43-45)

Trades at 14:04 (8), 14:09 (9) and 14:27 (10): windows `14:05`, `14:10` and `14:30`.
Windows `14:15`, `14:20` and `14:25` have no trade and do not exist (D-6): no output, no
crossover check, and the EMA does not decay across them. Window `14:30` is computed
directly from window `14:10`:

```
EMA38(14:30) = 10 * 2/39 + EMA38(14:10) * 37/39
```

### `DAY.NL`: overnight state and a trade reported in the next day's file (line 46)

Trade at 17:25 (40) in window `17:30`. It is completed by day 2 (below).

## Day 2: `trading-day-09-11-21.csv` (Tuesday)

### `DAY.NL` continued (lines 14 and 63)

- Line 14 trades at 09:00 (42), window day-2 `09:05`. The EMA carries over from day 1
  (D-8): `EMA38 = 42 * 2/39 + EMA38(day 1 17:30) * 37/39`, and likewise for EMA100.
- Line 63 is the file's last line. Its `Trading date` is **08-11-2021** while its `Date`
  is 09-11-2021 (a stale trading date), so it belongs to day 1's window `17:30`.
  At 17:26 it is later than line 46 (17:25), so it closes that window: **41 (day 2, line 63)**.
  The window draws two events from two files, which is why close candidates are merged
  across files.

### `SWING.FR`: a SELL, a quiet stretch, and a later BUY (lines 15-62)

One trade per window, at `13:02:30` plus 5 minutes per window, in windows `13:05` to `17:00`:

| window n | window end | close |
|---|---|---|
| 1 | 13:05 | 100 |
| 2-47 | 13:10-16:55 | 1 |
| 48 | 17:00 | 100 |

**Window 1.** From zero, `EMA38 = 200/39 ≈ 5.1282` and `EMA100 = 200/101 ≈ 1.9802`. The
previous values tie at 0, so **BUY** (D-7).

**Windows 2-45.** With a constant close of 1 the recurrence has a closed form, for n ≥ 1:

```
EMA_j(n) = 1 + (EMA_j(1) - 1) * (1 - alpha_j)^(n-1)
EMA38(n)  = 1 + (161/39)  * (37/39)^(n-1)
EMA100(n) = 1 + (99/101)  * (99/101)^(n-1)
```

EMA38 starts further above 1 but decays faster. It drops below EMA100 when
`(161/39) * (37/39)^m < (99/101) * (99/101)^m`, i.e. `m > ln(4.2116) / ln(1.03318) ≈ 44.05`,
so at m = 45: window 46. Until then EMA38 > EMA100 and nothing is advised.

**Window 46 (`16:50`), SELL.**

```
EMA38(45)  = 1.4071874826  >=  EMA100(45) = 1.4065574621
EMA38(46)  = 1.3863060732  <   EMA100(46) = 1.3985068193
```

**Window 47 (`16:55`).** Still below; no repeated SELL.

**Window 48 (`17:00`), BUY.** The close returns to 100. EMA38 moves by `2/39` of the gap and
EMA100 by only `2/101`, so EMA38 (≈ 6.42) jumps back above EMA100 (≈ 3.34).

Window 48 is also the last window of the input: a finite run must complete it (D-19).

## Advisories

Every symbol's first window is a BUY (D-7): `MULTI.FR 09:05`, `EDGE.NL 10:05`, `TIE.ETR
11:05`, `MISS.FR 12:05`, `SAME.FR 13:05`, `SAME.NL 13:05`, `SPARSE.ETR 14:05`, `DAY.NL
17:30` and `SWING.FR 13:05`. Then `SWING.FR` advises SELL at `16:50` and BUY at `17:00`:
**10 BUY and 1 SELL**.

## Late events

A streaming job admits an event only while its window is still open. Taking price events
one at a time in source order, with a watermark of *max event time seen − bound − 1*, the
smallest bound that admits an event in window W after a maximum M is `max(0, M − W + 1)`
(`oracle/README.md`). Only two events need a positive bound:

| event | window | maximum seen before it | smallest admitting bound |
|---|---|---|---|
| day 1, line 19 | 10:05 | 10:05:00.000 (line 18) | **1 ms** |
| day 2, line 63 | day 1 17:30 | day 2 16:57:30 (`SWING.FR` window 48) | **84,450,001 ms** (23 h 27 min 30 s + 1 ms) |

So `--watermark-bound-ms 0` rejects both, and the closes fall back to the next-latest trade:
`EDGE.NL 10:05` = **20 (line 17)** and `DAY.NL 17:30` = **40 (day 1, line 46)**.
`--watermark-bound-ms 1` rejects only line 63.

## Carry-forward

With `--empty-windows carry-forward` (D-42), a window without a trade repeats the symbol's previous close. Only two symbols have such windows between two of their trades:

- `SPARSE.ETR`: windows `14:15`, `14:20` and `14:25` close at **9**, the close of `14:10` (line 44). Window `14:30` then follows from `14:25`, not from `14:10`.
- `DAY.NL`: every window from day 1 `17:35` to day 2 `09:00`, 186 of them, closes at **41**, the close of day 1 `17:30` (day 2, line 63).

No window follows a symbol's last trade. That makes 189 more windows, 250 in all. Both gaps repeat a close at or above every earlier close of the symbol, so EMA38 stays above EMA100 and the advisories are the same 10 BUY and 1 SELL. `expected.json` has them under `variants.emptyWindowsCarryForward`.

## Binary `ticks`

`ticks-binary.tsv` lists every row the replayer sends from these two files: file index, line, the CSV value and the binary value of D-42 in hex. The replayer's `BinaryTicksTest` writes it (`-DregenerateFixtures=true`) and checks it. The job's and the correctness export's tests check that each binary value gives the same outcome as its CSV value.

## Counts

| | day 1 | day 2 |
|---|---|---|
| lines | 47 | 63 |
| comment | 13 | 12 |
| header | 1 | 1 |
| blank | 1 | 0 |
| malformedRow | 2 | 0 |
| nonPrice | 2 | 0 |
| malformedPrice | 1 | 0 |
| nonPositivePrice | 2 | 0 |
| missingTimestamp | 2 | 0 |
| malformedTimestamp | 2 | 0 |
| accepted | 21 | 50 |
| of which `Date` substituted | 1 | 0 |
| of which stale `Trading date` | 0 | 1 |
