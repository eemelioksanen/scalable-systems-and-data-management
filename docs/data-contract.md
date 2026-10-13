# Data contract

The original sample evidence below is preserved. [Monday's complete-day analysis](eda-monday.md) extends it, and [D-29](decisions.md#d-29-daily-analysis-interpretations-and-the-monday-checkpoint-2026-09-28-emilia-repo-issue-4) records the subsequently accepted UTC+2 interpretation, date fallback and window policies.

What the replayer (`producer/`), the Flink job (`flink-job/`) and the oracle (`oracle/`)
must all agree on about the input data, and the evidence for each claim.

Measured on a stratified sample of one weekday file. Regenerate with:

```bash
analysis/src/fetch_slice.sh
analysis/run.sh analysis/src/profile.py
```

Full tables: [`analysis/out/profile.md`](../analysis/out/profile.md). Provenance:
[`analysis/out/provenance.json`](../analysis/out/provenance.json).

## 1. Provenance and sample

| | |
|---|---|
| Source | <https://zenodo.org/records/6382482> (DOI `10.5281/zenodo.6382482`) |
| Licence | **CC BY-NC-SA 4.0** — non-commercial, share-alike, attribution required |
| Attribution | Frischbier, Tahir, Doblander, Hormann, Mayer, Jacobsen — *DEBS 2022 Grand Challenge Data Set: Trading Data* |
| File profiled | `debs2022-gc-trading-day-08-11-21.csv` (Mon 08.11.2021) |
| File size / MD5 | 4,672,671,094 bytes / `a08013f09a6b77355dae8b3ce141624d` |
| Sample | 1,015,019,421 bytes — **21.72%** of the file, 11,784,671 data rows |
| Sampling method | 21 byte ranges: an 8 MiB head plus 20 × 48 MiB evenly spaced |

**Storage.** The seven daily files total ≈ 25 GB (weekdays 4.67–5.51 GB each; 13.11 and
14.11 are a weekend and are 3.1 MB and 1.9 MB). A full-week Kafka preload needs roughly
the same again, so plan for ≥ 100 GB free before pulling the whole set. Nothing bulky is
committed: `data/` is gitignored.

**Why stratified rather than a head slice.** Rows are in system-time order, so a prefix
would cover only the pre-open and the opening auction — the busiest and least typical part
of the session. Evenly spaced ranges cover the whole day for a fifth of the download.
Chunks are kept as **separate files** because source order is contiguous only *within* a
chunk; any ordering analysis that spanned a boundary would be comparing rows hours apart.

## 2. File structure

| Property | Measured |
|---|---|
| Comment lines (leading `#`) | 12 — eleven before the header, **one after it** |
| Header rows | 1, at line 12 |
| Declared columns | **39** |
| Fields per data row | **40** — every row ends with a trailing delimiter |
| Rows with the trailing empty field | 11,784,671 (**100%**) |
| Malformed rows (neither 39 nor 40) | **0** |
| Quoting | **none** — 0 rows contain a double quote |
| Null representation | empty field |
| Line endings | CRLF on the 11 leading comment lines, **LF everywhere else** |

Two traps here, both of which reject the entire file if handled naively:

- **The trailing delimiter.** Data rows carry an unnamed empty 40th field. A strict
  39-column parser rejects 100% of rows. Parsers must accept 40 fields when the last is
  empty, and drop it.
- **The comment line after the header.** A parser that skips a fixed number of leading
  comment lines will read `#'Unique ID','Security Type…'` as data. Skip on the `#`
  prefix, not on a line count.

Column order (39 names) is `ID, SecType, Date, Time, Ask, Ask volume, Bid, Bid volume,
Ask time, Day's high ask, Close, Currency, Day's high ask time, Day's high, ISIN, Auction
price, Day's low ask, Day's low, Day's low ask time, Open, Nominal value, Last, Last
volume, Trading time, Total volume, Mid price, Trading date, Profit, Current price,
Related indices, Day high bid time, Day low bid time, Open Time, Last trade time, Close
Time, Day high Time, Day low Time, Bid time, Auction Time`. Several header names carry a
leading space; match on position, not on a trimmed name.

## 3. Row taxonomy

| Class | Rows | Share | Definition |
|---|---|---|---|
| `empty_payload` | 5,840,242 | 49.56% | Every payload column empty — housekeeping |
| `quote` | 3,347,501 | 28.41% | No `Last`, but `Ask` or `Bid` present |
| `price` | 2,389,887 | **20.28%** | `Last` non-empty and > 0 — **the only rows the query consumes** |
| `other` | 189,446 | 1.61% | Partially populated, no `Last`, no quote |
| `price_zero` | 17,593 | 0.15% | `Last` present but exactly `0.000000` |
| `zero_filled` | 2 | ~0% | `Close` and `Open` both exactly 0 |

By exchange: **FR 6,409,174 (54.4%), NL 3,204,493 (27.2%), ETR 2,171,004 (18.4%)** across
5,493 symbols — consistent with the 55/28/17 split stated for the full dataset, which is a
useful check that the stratified sample is not skewed.

By security type: equities 9,779,904 rows (83.0%), indices 2,004,767 (17.0%). But the
**price** rows invert that: indices contribute 1,968,777 and equities only 421,110, a 4.7:1
ratio. Indices tick steadily; most equity traffic is quotes and housekeeping. Any
per-symbol load model must use price-event counts, not raw row counts.

**`price_zero` is excluded from price events.** A `Last` of exactly `0.000000` is a
populated field, not a traded price; feeding it to the EMA would drag the average to zero.
This refines D-2, which said only "`Last` is non-null".

## 4. Symbol key

`ID` is used **as-is** as the key. All 11,784,671 rows match `^[A-Za-z0-9]+\.[A-Za-z]+$`
with **zero exceptions**, across 5,493 distinct symbols. The suffix is the exchange
(`FR`, `NL`, `ETR`) and is derived from the ID rather than stored separately. Symbols are
alphanumeric and may be entirely numeric (`846741.ETR`), so a key must never be parsed as
a number.

## 5. The trade price is `Last`

The feed carries six price-shaped columns. Population across all 11,784,671 rows:

| `Last` | `Close` | `Current price` | `Ask` | `Bid` | `Mid price` |
|---|---|---|---|---|---|
| 2,407,480 | 5,178 | **0** | 1,776,156 | 1,782,616 | 239,484 |

- `Current price` is **never populated**, anywhere.
- `Close` appears on 5,178 rows and is **always exactly zero**; it never carries a usable
  price, and 0 rows have a positive `Close` without a `Last`.
- `Close` and `Last` **never co-occur**: all 2,389,887 price rows have an empty `Close`.

So `Close` in this feed is not an intra-day five-minute close and cannot substitute for
one. **The five-minute window close is the `Last` of the window's latest trade**, and
`Ask`/`Bid`/`Mid price` are quote data that the query does not consume.

## 6. Timestamps

### Precision

`Trading time` is `HH:MM:SS.mmm` — **three** fractional digits on **100%** of price rows
(2,389,887 / 2,389,887). This corrects `ROADMAP.md`, which specified `HH:MM:SS.ssss`.

**Consequence: epoch milliseconds are lossless.** There is no sub-millisecond precision to
preserve, so event time needs no microsecond companion field and Flink's millisecond
timers can carry the full resolution of the source. `Last` always has exactly 6 decimals.

### Constructing event time

`Trading time` is present on **every** price row. `Trading date` is **absent on 2,201,703
of 2,389,887 price rows (92.1%)**, and where it *is* present it **always equals `Date`**
(0 rows disagree). The date part therefore falls back to the system `Date`, which is safe
by measurement rather than by assumption.

Observed coverage in the sample: system `Time` 07:00:00.000–17:19:30.000, `Trading time`
07:00:00.021–17:19:30.308.

### An anomaly, recorded but not explained

`Time` minus `Trading time` over all 2,389,887 price rows: min −1011 ms, p50 **−310 ms**,
p99 0 ms, max +300 ms, with **2,078,796 (87.0%)** negative. The system update timestamp
usually *precedes* the trade timestamp it reports, which is the opposite of what a
capture pipeline should produce. This does not affect the contract — event time is built
from `Trading time` either way — but it is not yet explained, and the watermark bound must
not be derived from it until it is. Left to the fuller EDA.

## 7. Ordering, ties, and source position

This is the measurement that decides the close rule.

| | Measured |
|---|---|
| Consecutive per-symbol price pairs (within a chunk) | 2,325,956 |
| Pairs going **backward** in `Trading time` | **257,149 (11.06%)** |
| Pairs with **equal** `Trading time` | 128,571 |
| Same symbol + identical `Trading time` groups | 57,096 (185,797 rows) |
| …of those, carrying **differing prices** | **9,981 (17.5%)** |

Two conclusions, both load-bearing:

1. **File order is not trade order.** 11% of consecutive price rows for a symbol step
   backward in trading time. "The last row to arrive" and "the trade with the latest
   event time" are therefore materially different rules, and D-4's choice of the latter
   is not a formality.
2. **Ties are real and conflicting.** 9,981 groups share a symbol and a millisecond but
   disagree on price, so max-by-event-time alone is not deterministic. A tie-break is
   required for reproducibility.

**The tie-break is source position**, defined as:

- *Replayer / Flink:* the Kafka offset. The key is the symbol and the replayer preserves
  file order per symbol, so all of a symbol's events land in one partition and the offset
  is a total order over them.
- *Oracle:* `(file date, line number)`.

Both express the same underlying order: position in the source file. The window close is
`max by (event_time, source_position)`.

## 8. Replay envelope

| | |
|---|---|
| Unit | one CSV data row → one Kafka record on `ticks` |
| Skipped | lines beginning with `#`, and the single header row |
| Key | `ID`, verbatim |
| Value | a **projection of 7 of the 39 columns** (see below) |
| Record timestamp | replayer wall-clock send time, for latency measurement |
| Ordering | day files in date order, lines in file order, per-symbol order preserved |

The `ticks` value carries `ID, SecType, Date, Time, Last, Trading time, Trading date` —
the 7 columns the query needs. **This is a projection, not the raw line**: `interfaces.md`
D-13 described it as "7 fields copied verbatim from the CSV", which reads as though the
file had 7 columns. It has 39 (40 with the trailing delimiter). The replayer selects; it
does not parse, normalise or classify — that stays in Flink, so normalisation is part of
the system under test.

## 9. Result fields

Unchanged from [`interfaces.md`](interfaces.md) — `ema-results` carries `symbol, secType,
windowEnd, close, ema38, ema100, maxIngestTs, emitTs`; `advisories` carries the same with
`type` (`BUY`/`SELL`) in place of `close`. Two points this profile pins down:

- `secType` is exactly `E` or `I`; no other value occurs.
- `windowEnd` is epoch **milliseconds**, which §6 shows is lossless for this data.

Expected values follow the hand-worked sequences in
[`fixtures/golden/derivation.md`](../fixtures/golden/derivation.md).

## 10. Follow-up after the sample

| Topic | Why it is not settled here |
|---|---|
| Watermark bound and idleness | The [full-week analysis](eda-week.md) left the runtime setting open. D-30 demonstrates a sufficient 64,265,001 ms source-order bound; 264 ms and 60 s lose 1,812 windows. D-38 proposes the defaults after paced and preloaded runs: that bound without idleness for a finite replay, 1 s and 30 s for a live job. |
| Missing / stale timestamp policy | D-29 accepts the missing-date fallback, excludes unparseable event timestamps, and preserves valid explicit dates. The interpretation of Wednesday's summary-like rows remains open. |

D-29 accepts the skip-empty-window, carry-over, late-event and finite-input policies as interpretations of the spec.
