# Daily dataset analysis

Issue [#4](https://github.com/eemelioksanen/scalable-systems-and-data-management/issues/4), starting with the complete Monday file.
The first run stops after Monday for review; the full week is still pending.

```bash
bash analysis/daily.sh 2021-11-08
```

The host downloads just that day into ignored `data/daily/`, resumes an interrupted transfer, and verifies its published MD5 and a local SHA-256.
Docker runs the analysis with 4 GiB memory, four CPUs, two Polars parsing threads, no network, a read-only filesystem and read-only raw data.
The parser hands Polars at most 100,000 physical lines at a time and combines temporary Parquet parts into the reusable checkpoint.
DuckDB has a 2 GB buffer limit and up to 12 GB of disk spill; the container limit also covers allocations outside DuckDB's buffer manager.
The download requires 20 GiB of spare space beyond the remaining transfer.
Monday's raw data and checkpoints fit within 10 GB; allow more when retaining later days.

Outputs:

- `data/daily/*.json`: download provenance and checksums.
- `data/analysis/YYYY-MM-DD/rows.parquet`: all physical lines, classified and projected, in a streaming checkpoint.
- `data/analysis/YYYY-MM-DD/parsed.json`: input, parser, timezone and Polars identities plus the checkpoint checksum. A rerun reuses it only if all match.
- `data/analysis/YYYY-MM-DD/working.duckdb`: disposable analysis database, rebuilt from the checkpoint on rerun.
- `data/analysis/YYYY-MM-DD/{rates_second,windows,late_candidates_8}.parquet`: detailed rate, close and lateness tables.
- `analysis/out/daily/YYYY-MM-DD/`: reviewable CSV summaries, PNG/SVG figures and a completion manifest. The manifest is written only after all steps succeed.

The manifests include the base revision, worktree status, hashes of all source scripts, dependency versions, input identity, output hashes, runtime and process peak RSS.
They preserve the original checkpoint's parsing time and peak RSS even when a later run reuses it.
Raw data and large intermediate files stay outside Git.
The existing `analysis/out/` sample remains a separate investigation; its selected-column definition of an empty payload differs from this analysis's all-column definition.

For local iteration, install `analysis/requirements.txt` in a Python 3.12 virtualenv, then run:

```bash
analysis/.venv/bin/python analysis/src/daily.py \
  data/daily/debs2022-gc-trading-day-08-11-21.csv --day 2021-11-08
analysis/.venv/bin/python -m unittest discover -s analysis/tests -v
analysis/.venv/bin/python analysis/src/daily_plot.py analysis/out/daily/2021-11-08
```

The native path uses DuckDB's buffer limit but has no container memory cap.
Figures regenerate from the saved CSV summaries without reading the raw CSV.
Pass `--zone-offset=+01:00` to compare the alternative timezone; the default is the accepted literal CEST offset `+02:00`.
Use separate output and cache directories for concurrent runs.

## Definitions

Every physical line is a comment, header, blank, malformed row, or one mutually exclusive event class.
Price rows have finite positive `Last`; zero, negative and malformed prices have separate counts.
Quote rows have no `Last` and at least one populated `Ask` or `Bid`.
An empty payload has all 35 columns after `Time` empty. Remaining populated rows are `other`.
Quotes and non-price rows still count toward raw replay throughput.

Event time uses `Trading date` and `Trading time`, with system `Date` substituted only when trading date is absent.
Unparseable event timestamps are excluded and counted; valid explicit dates are preserved, including date mismatches.
Finer-than-millisecond fractions are truncated, matching D-26; precision distributions remain visible.
The five-minute close maximizes `(event_ms, physical source line)` within a symbol/window.
Empty windows produce no update, EMA state carries across day boundaries, late events are discarded and counted, and finite input flushes remaining windows.
This analysis calculates closes, not EMAs or advisories. The independent oracle owns those calculations.

The course brief labels `Time` as system time of the last received update and `Trading time` as the time of the last bid/ask/trade update.
Rates by `Date`/`Time` are therefore recorded-system-time rates, a proxy for arrival rates, not measured network ingestion rates.
Missing system timestamps are excluded from rate bins and reconciled separately.
No ordering calculation sorts by system or trading time: physical file order is the arrival-order model.

Rate p99 uses the discrete empirical quantile over every integer second from the first through the last valid system timestamp, inclusive, including zero-event seconds.
The all-exchange rate aggregates counts per second before calculating quantiles.
Per-minute plots fill missing minutes with zero.
Peak timestamps are recorded in `peak_seconds.csv`; opening/closing peaks are observations, not an inferred official exchange calendar.
`trading_rates_summary.csv` computes the same statistics for accepted price events on the trading clock.
Its interval includes preserved explicit dates outside the selected day. A distant date can lengthen the interval and lower p99, so inspect `date_mismatch` counts and the reported first/last seconds before using it as a replay target.
The replay targets use recorded system time. Trading-clock summaries are diagnostic and must be revisited if a later day contains date mismatches.
`activity_spikes.csv` compares recorded 07:00–10:00 and 16:00–19:00 bands, including every second in those bands.
These bands bracket the plotted morning and evening activity; they are analysis intervals, not official session definitions.
`replay_targets.csv` records 1×, 10× and 100× p99 and peak offered-load targets.

Empty-window tables report both the full local calendar day, 288 windows, and the exchange's observed system-time span rounded outward to five-minute boundaries.
Both denominators include every symbol observed that day, including symbols without price events.
Each symbol appears once; if it has multiple security types, the `sec_type` column lists them with `|` separators.
An observed exchange span is not a trading-session calendar. A window is occupied only if it has an accepted price event inside that interval.
Nonempty windows with explicit dates outside the selected calendar day remain counted separately through the total nonempty-window column.

Gini and top-k share use all observed symbols, including zero-price symbols, separately for raw and accepted price events.
Partition load uses UTF-8 symbol keys and Kafka's positive Murmur2 hash modulo 1, 2, 4, 8, 16 or 32 partitions.
The test vectors were obtained by running Kafka's `Utils.murmur2` and `Utils.toPositive` from the local job jar in a JDK container; short, numeric and UTF-8 keys are included.

## Watermark limits

`partition_disorder.csv` measures running-maximum disorder within each Kafka partition, including progress caused by other symbols.
It also counts adjacent backward jumps per partition; `symbol_disorder.csv` does the equivalent per symbol.
`symbols.csv` and `partition_timestamp_quality.csv` locate missing dates, invalid timestamps, date mismatches and gaps over two seconds at both levels.
Those gaps compare system time with trading time on the same row; they are not intervals between consecutive events.
The system-time backward-step table groups each jump by the current row's exchange and category; the preceding row is the preceding data row in the file, potentially from another exchange.
For a window with exclusive end `W`, a prior partition maximum `M` and bound `B`, Flink's bounded generator can emit `M-B-1`.
The existing operator rejects events when `windowEnd-1 <= watermark`; the smallest admitting integer bound in this model is `max(0,M-W+1)`.
Being behind the watermark does not alone make an event late if its window is still open.

`watermark_bounds.csv` applies that partition-local watermark before each event, then compares the surviving closes with the admit-all closes.
It reports discarded events, missing windows and changed existing closes separately.
This is an aggressive per-partition model. With all partitions active, the downstream watermark is their minimum, so it can lag this model.
Periodic emission and a fast preload can admit more events.
Changed-close counts describe the particular model; they are not a bound on changed closes under every possible schedule.

Idleness invalidates the active-partition guarantee: a resumed partition may face a watermark advanced while it was excluded.
D-28 records 209 late events after a pause in an eight-partition run, despite small data disorder.
The full-week oracle also found Wednesday summary-like rows that short-bound paced runs can reject while a preload admits them.
The analysis preserves those rows under the selected policy; it does not silently classify them away.
The eventual correctness comparison must use the events actually admitted by the job, and a final bound requires full-week and runtime evidence.

Each day is an independent diagnostic run, starting with no previous partition maxima.
Combining daily summaries is not a full-week ordering simulation: cross-file watermark progress and windows receiving events from multiple files still require a continuous full-week analysis.

## Sources

- [DEBS dataset and checksums](https://zenodo.org/records/6382482).
- [Kafka keyed partitioning](https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L302).
- [Flink Kafka watermarks and idleness](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/#event-time-and-watermarks).
- [Polars streaming Parquet sink](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html).
- [DuckDB memory management](https://duckdb.org/2024/07/09/memory-management).
- [Oracle evidence](../oracle/evidence/README.md) and [D-28 runtime findings](../docs/decisions.md#d-28-query-2-and-subscriptions-2026-09-28-leo-immonen-issue-8).
