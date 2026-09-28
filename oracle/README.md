# Oracle

A batch implementation of Query 1 (five-minute closes, EMA38 and EMA100) and Query 2
(BUY/SELL crossovers) for the correctness check (D-18). It computes every symbol before
any subscription filtering and writes Parquet for the full week.

It is built from the recorded contract only: [`docs/data-contract.md`](../docs/data-contract.md),
[`docs/decisions.md`](../docs/decisions.md) and [`docs/interfaces.md`](../docs/interfaces.md).
It does not read or mirror the Flink job's code, so agreement between the two is evidence
rather than a shared bug. Keep it that way when changing either.

## Running

Only Docker is needed. The container is the canonical environment; `USE_VENV=1` runs the
same code in a local venv.

```bash
oracle/run.sh data/debs2022-gc-trading-day-*.csv   # results in oracle/out/
oracle/run.sh test                                 # unit and fixture tests
```

Day files must lie under `DATA_DIR` (default `data/`), which the container mounts
read-only. Pass `--out /out/<name>` to write to `oracle/out/<name>/`. The input order is
taken from the `DD-MM-YY` date in each file name, not from the command line.

| Option | Default | Meaning | Flink job flag |
|---|---|---|---|
| `--zone-offset` | `Z` | UTC offset of the trading timestamps (D-3) | `--zone-offset` (D-19) |
| `--missing-trading-date` | `system-date` | `system-date` substitutes `Date` for a missing `Trading date` (D-25); `drop` counts the event as missingTimestamp (the D-10 alternative) | none: the job always applies D-25 (D-26) |
| `--watermark-bound-ms` | unset | reject the events a streaming run with this bound would treat as late, under the model below | `--watermark-bound-ms` |
| `--exclude-events` | unset | CSV or Parquet of `(sourceFile, sourceLine)` to reject, e.g. the job's own late events | |
| `--no-input-checksums` | | skip the MD5 of each input | |
| `--no-precision-check` | | skip the 50-digit decimal recomputation | |

Changing the timezone (D-3) or the missing-timestamp policy (D-10) is a re-run with a
different flag, not a code change.

## Semantics

| Rule | Implementation | Decision |
|---|---|---|
| CSV layout | Each physical line is read whole; `#` lines and the header row are skipped by content. A data row has 39 fields, or 40 with an empty last one; no quoting; empty field = null; columns by position. | D-21 |
| Symbol | `ID` verbatim. `exchange` is the suffix after the last `.`. | D-1 |
| Price event | `Last` non-empty and > 0. | D-2 |
| Event time | `Trading date` (or `Date`, see above) + `Trading time`, as epoch ms at `--zone-offset`. Digits beyond the millisecond are truncated. | D-3, D-22, D-25, D-26 |
| Window | Tumbling, 5 minutes, aligned to the epoch; identified by its exclusive end `windowEnd`. | D-16 |
| Close | `Last` of the window's event with the greatest `(eventTime, file date, line)`. A window may draw events from two files. | D-4, D-23 |
| Empty window | Does not exist: no output, EMA unchanged, no crossover check. | D-6 |
| EMA | `EMA_j = close * 2/(1+j) + EMA_j,prev * (1 - 2/(1+j))`, j = 38, 100, from `EMA_j,0 = 0`, carried across days. | D-7, D-8 |
| Advisory | BUY iff `EMA38 > EMA100` and `EMA38,prev <= EMA100,prev`; SELL iff `EMA38 < EMA100` and `EMA38,prev >= EMA100,prev`. A current tie advises nothing. | spec, D-16 |
| Completion | Every window with a trade is emitted, including the last one of the input. | D-19 |

Every physical line gets exactly one category, counted per file: `comment`, `header`,
`blank`, `malformedRow` (wrong field count, or empty `ID`), `nonPrice` (empty `Last`),
`malformedPrice` (`Last` not a decimal number), `nonPositivePrice`, `missingTimestamp`
(no `Trading time`, or no date under the chosen policy), `malformedTimestamp`
(an impossible date or time) and `accepted`. Accepted events are further counted as
`systemDateEvents` (D-25 applied) and `staleTradingDateEvents` (`Trading date` ≠ `Date`).

## Outputs

All in `oracle/out/` (gitignored), sorted, and written with fixed Parquet settings.

| File | Rows | Columns |
|---|---|---|
| `ema_results.parquet` | one per (symbol, window with a trade) | D-18: `symbol, secType, windowEnd, close, ema38, ema100`; diagnostics: `exchange, priceEvents, closeEventTime, closeSourceFile, closeSourceLine` |
| `advisories.parquet` | one per crossover | D-18: `symbol, secType, windowEnd, type, ema38, ema100`; diagnostic: `exchange` |
| `crossover_summary.parquet` | per (trading day, exchange, secType, type) | `advisories` |
| `window_summary.parquet` | per (trading day, exchange, secType) | `windows, symbols, priceEvents` |
| `symbol_summary.parquet` | per symbol | `exchange, secTypes, windows, priceEvents, firstWindowEnd, lastWindowEnd, buys, sells` |
| `input_counts.parquet` | per input file | every category above, plus `excluded`, `lateRejected`, `admitted` |
| `rejected_events.parquet` | per event dropped by `--exclude-events` or `--watermark-bound-ms` | `sourceFile, sourceLine, symbol, secType, eventTime, windowEnd, price, minBoundMs, rejection` |
| `manifest.json` | | provenance: revision, command, parameters, input sizes and MD5s, all counts, the admission profile, the precision report, output row counts and checksums, timings, peak memory |

The trading day in the summaries is the date of the window's last millisecond at `--zone-offset`.

## Late events: matching a batch run to a streaming run

The oracle sees every event. A streaming job does not: it rejects an event whose window
it has already evaluated (D-5, D-19). Agreement must not come from quietly dropping the
results that differ, so rejections are counted and kept apart from parse rejections.

**The admission model.** Take the price events one at a time in source order (day files
by date, lines in order), with a bounded-out-of-orderness watermark of *maximum event time
seen − B − 1*. A window ending at W is evaluated once the watermark reaches W − 1, so an
event in that window is late iff the maximum M seen before it satisfies M − B ≥ W. The
smallest bound that admits it is therefore

```
minBoundMs = max(0, M - W + 1)
```

The oracle computes this for every event, and `manifest.json` reports how many events each
bound would reject (`admission.lateEventsByBoundMs`) and the smallest bound that rejects
none (`admission.smallestBoundWithNoLateEventsMs`).

**Why it is conservative.** In the job each Kafka partition carries a subsequence of the
source order (the key is the symbol and the replayer keeps per-symbol order, D-23). When
the window operator processes an event, its watermark is the minimum over partitions of
what each has emitted, which is at most the maximum over the events of the event's own
partition that precede it, which is at most M. Periodic watermark emission only lowers it
further. So an event the job rejects at bound B is also rejected by the model at bound B.
The one exception is idleness: a partition marked idle is left out of the minimum, and an
event arriving on it afterwards can be late in the job but not in the model.

**How to compare.**

1. If `lateEventsByBoundMs` is 0 at the job's bound, and the job's `lateTicks` counter is
   0, compare the job against the default (admit-all) oracle output directly.
2. Otherwise, pass the job's late events to `--exclude-events` as `(sourceFile, sourceLine)`,
   which reproduces exactly the set of events the job admitted. This needs the replayer to
   carry source identity (#6).
3. `--watermark-bound-ms` applies the model itself, as an estimate of what a run at that
   bound loses when the job's own late events are not available.

In every case the rejected events are in `rejected_events.parquet`, counted per file as
`excluded` or `lateRejected`, and the comparer (#20) reports them beside the diff.

## Precision and the comparison tolerance

The EMAs are computed in IEEE 754 binary64, with the spec's formula written exactly as
above and `2/(1+j)` rounded once. `close` is `Last` parsed from its six-decimal text,
correctly rounded, as `Double.parseDouble` also does, so `close` compares exactly (D-18).

The manifest's `precision` section recomputes every EMA in 50-digit decimal arithmetic from
the same closes and reports:

- `max_abs_error_ema38`, `max_abs_error_ema100`, `max_error_ulps`, `worst_symbol`: how far
  binary64 drifts from the exact recurrence.
- `windows_over_tolerance`, `symbols_over_tolerance`: windows where that drift alone exceeds
  the D-18 tolerance of 1e-9.
- `advisory_disagreements`: windows whose BUY/SELL decision differs between the two
  arithmetics. It must be 0 for the advisories to count as exact.
- `near_ties`: windows with `|EMA38 − EMA100| <= 1e-9`, where another correct
  implementation could reach the opposite decision by rounding differently.

The tolerance is absolute, and the largest index values reach about 1.1 × 10⁶, where one
ulp is about 1.2 × 10⁻¹⁰, so 1e-9 is only about eight ulps there. On 08-11-21 the binary64
recurrence stays within 24 ulps of exact, but that is 1.4 × 10⁻⁹ for the largest values,
and 50 windows in 2 symbols exceed 1e-9 on drift alone. An implementation that evaluates the
same formula in a different order can differ from the oracle by more than 1e-9 on those
symbols without either being wrong. The comparer should show the magnitude of every EMA
mismatch so that this case is recognisable.

## Determinism and memory

Outputs are sorted by `(symbol, windowEnd)` and written with pinned Polars and fixed
Parquet settings. `manifest.json` records a SHA-256 of each file and of its rows as CSV
text; the second survives a Polars upgrade, the first does not. A test runs the fixtures
twice and compares both.

Each day file is streamed by the Polars CSV reader and reduced to one close candidate per
(symbol, window) before the next file is read, so memory is bounded by one day's price
events (about 12 M) plus the week's windows (a few million), not by the input size.
`peakAnonMemoryMb` in the manifest is the peak anonymous memory; it leaves out the page
cache of the memory-mapped CSV, which `ru_maxrss` would count.

## Evidence

[`evidence/`](evidence/) keeps the manifest of each recorded run, with the command that
produced it. The fixtures the tests check against are in
[`fixtures/semantics/`](../fixtures/semantics/derivation.md) (hand-derived, this issue)
and [`fixtures/golden/`](../fixtures/golden/derivation.md) (EMA arithmetic, #18).
