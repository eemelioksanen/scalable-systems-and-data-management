# Decision log

Record every design decision here, with who made it, when, and why. The report's architecture section and each member's individual-contribution answers are written from this log.

Status is one of: **proposed**, **accepted**, **superseded (by D-n)**.

## Query semantics (week 1)

The Flink job (`flink-job/`) and the oracle (`oracle/`) must implement exactly these. Accepting a proposal requires EDA evidence (see `docs/ROADMAP.md` §3).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-1 | Symbol key | `ID` column as-is (e.g. `RDSA.NL`). `exchange` is derived from the suffix. | **accepted** | Confirmed: 11,784,671 rows / 5,493 symbols all match `^[A-Za-z0-9]+\.[A-Za-z]+$`, zero exceptions. Symbols may be fully numeric (`846741.ETR`), so never parse a key as a number. |
| D-2 | Price event | A row whose `Last` is non-empty **and > 0**. All other rows are quote or housekeeping events: counted, then dropped before `keyBy`. | **accepted (refined)** | 20.28% of rows are price events. 17,593 rows carry `Last` = `0.000000`; these are excluded, since feeding a zero into the EMA would drag the average down. The original wording ("non-null") would have included them. |
| D-3 | Event time | `Trading date` + `Trading time` parsed to epoch ms, with `Date` substituted when `Trading date` is absent (D-23). One timezone interpretation, used everywhere. | **accepted, timezone open** | `Trading time` is `HH:MM:SS.mmm` on 100% of price rows, so epoch ms is lossless (D-20). The CET/CEST reading is a spec-wording question for the course staff; any whole-hour offset preserves window alignment, so it shifts published `windowEnd` values but not which trades share a window. |
| D-4 | Close | The price with the latest event time in the window. Ties broken by source position (D-21). | **accepted** | Load-bearing, not a formality: **11.06%** of consecutive per-symbol price pairs (257,149 / 2,325,956) run *backward* in `Trading time` within file order, so "latest event time" and "last to arrive" genuinely differ. 9,981 same-millisecond groups carry **differing** prices, so a tie-break is required for determinism. |
| D-5 | Window firing | Fire when the event-time watermark passes the window end. Bounded out-of-orderness with idleness. Late events go to a side output and are counted. | proposed | Bound still unset. Deliberately not derived from this sample: it needs a full day rather than 21.7% of one, and the unexplained `Time` − `Trading time` anomaly (p50 **−310 ms**, 87% negative) must be resolved first. |
| D-6 | Empty windows | No price event in a window: EMA unchanged, no output, no crossover check. | proposed | The carry-forward alternative is an ablation. |
| D-7 | EMA init | EMA₀ = 0 exactly as the spec says. | proposed | Causes a BUY in every symbol's first window. Flag it in the viz and report. |
| D-8 | Day boundaries | EMA state carries over across days and the weekend. | proposed | |
| D-9 | Subscriptions | EMAs are computed for all symbols. Results are emitted only for subscribed symbols, via a compacted `subscriptions` topic and broadcast state. | proposed | |
| D-10 | Missing or stale timestamps | Drop them, or fall back to `Date`+`Time`? | open | Measured: `Trading time` is never missing on a price row; `Trading date` is missing on **2,201,703 / 2,389,887 (92.1%)**, and where present **always equals `Date`** (0 disagreements). That evidences the D-23 fallback, but the policy stays open pending the fuller EDA. |

## Stack and infrastructure

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-11 | Stream processor | Apache Flink 2.2.1 (Java 17) + Kafka 4.3.1 (KRaft). | accepted | Event-time windows, keyed state, horizontal scaling. The Kafka connector (`5.0.0-2.2`) is not yet released for Flink 2.3. |
| D-12 | Build | Maven runs inside Docker (multi-stage `Dockerfile`). The IDE runs in a dev container. No JDK on hosts. | accepted | |

## Interfaces

Accepted 2026-09-28. The exact formats are in [interfaces.md](interfaces.md).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-13 | `ticks` content | The replayer forwards every CSV row as 7 raw columns (`ID, SecType, Date, Time, Last, Trading time, Trading date`). All parsing and normalization happen in Flink. | accepted | Normalization belongs to the system and is described in the report. Early filtering in the replayer is a later design comparison. |
| D-14 | `ticks` encoding | UTF-8 CSV string, verbatim fields, empty = null, no quoting. | accepted | Compare against a binary encoding later as a design comparison. |
| D-15 | `ticks` key and partitions | Key = symbol, default partitioner, 8 partitions locally. The record timestamp is the replayer's send time. | accepted | Revisit the partition count for the cloud runs. The Kafka offset gives per-symbol arrival order for D-4 ties. |
| D-16 | Output records | JSON. `ema-results`: `symbol, secType, windowEnd, close, ema38, ema100, maxIngestTs, emitTs`. `advisories`: `symbol, secType, windowEnd, type (BUY/SELL), ema38, ema100, maxIngestTs, emitTs`. | accepted | Which latency definition to report is still open. `secType` enables equity/index filtering in the viz. |
| D-17 | `subscriptions` | Key = trader ID. Value = JSON symbol list (`["*"]` = all). A tombstone removes the trader. Compacted topic. | accepted | Results are emitted once per symbol. Fan-out to traders happens in the viz backend. |
| D-18 | Correctness check | The oracle writes Parquet with the same field names. EMAs are joined on (symbol, windowEnd) with 1e-9 absolute tolerance and exact `close`. Advisories are compared as sets of (symbol, windowEnd, type), reporting precision and recall. | accepted | Flink runs with a `["*"]` subscription for the check. |

## Log

Add later decisions below, newest first.

### D-23: `Date` substitutes for a missing `Trading date` (2026-09-28)
Context: `Trading date` is absent on 92.1% of price rows in the profiled sample.
Decision: build the date part of event time from `Trading date` when present, otherwise from the system `Date`. The time part is always `Trading time`, which is never missing on a price row.
Alternatives considered: dropping rows without a `Trading date` would discard 92% of all trades. Using `Date` unconditionally was rejected only for lack of evidence, not merit — the two never disagree where both are present.
Consequences: **proposed**, not accepted; it is the concrete form D-10 will take if the fuller EDA agrees. The oracle and the Flink job must apply it identically or they diverge on nearly every row.

### D-22: `ticks` carries a projection, not the raw line (2026-09-28)
Context: D-13 says the replayer forwards "7 fields copied verbatim from the CSV", which reads as though the file had 7 columns.
Decision: wording clarified. The file has **39 named columns** (40 fields per row, see D-19). The replayer emits a **projection** of 7 of them — `ID, SecType, Date, Time, Last, Trading time, Trading date` — selected but not parsed, normalised or classified.
Consequences: clarifies D-13 without changing the wire format. Normalisation stays inside Flink, so it remains part of the system under test.

### D-21: source position is the tie-break (2026-09-28)
Context: D-4 needs a deterministic order among trades sharing an event time.
Decision: tie-break on position in the source file. For the replayer and Flink that is the Kafka offset (key = symbol, so a symbol's events occupy one partition and offsets totally order them); for the oracle it is `(file date, line number)`. The close is `max by (event_time, source_position)`.
Evidence: 57,096 groups share a symbol and a millisecond; **9,981 of them disagree on price**, so max-by-event-time alone is not deterministic.
Consequences: the replayer must preserve per-symbol file order, and any ordering analysis must stay within one sample chunk, since source order is contiguous only inside a chunk.

### D-20: event time is epoch milliseconds, with no sub-millisecond companion (2026-09-28)
Context: we considered carrying an `eventTimeMicros` field so that a window's latest trade could be chosen even though Flink's timers are millisecond-based.
Decision: rejected as unnecessary. `Trading time` is `HH:MM:SS.mmm` — **three** fractional digits on 100% of price rows — so epoch ms is lossless and Flink's native timers carry the source's full resolution.
Alternatives considered: (a) an extra `eventTimeMicros` long, now dead weight; (b) microseconds as the event-time unit throughout, which would have broken `Time.minutes(5)`, Kafka's millisecond record timestamps and Flink's own watermark-lag metrics.
Consequences: supersedes the `HH:MM:SS.ssss` precision stated in `ROADMAP.md`. Ties still occur *within* a millisecond, which is why D-21 is still required.

### D-19: CSV structural tolerances (2026-09-28)
Context: the real file departs from its own header in ways that reject every row if parsed strictly.
Decision: parsers must (a) accept **40** fields per data row — a trailing delimiter adds an unnamed empty field to 100% of rows — and drop the last one; (b) skip comment lines by the `#` prefix rather than by a fixed count, because one comment line sits **after** the header; (c) expect no quoting anywhere (0 rows contain a double quote); (d) treat an empty field as null; (e) tolerate CRLF on the 11 leading comment lines and LF everywhere else; (f) match columns by position, as several header names carry a leading space.
Evidence: 0 malformed rows out of 11,784,671 once these hold; 100% before.
Consequences: applies equally to the replayer, the oracle and the EDA. A tool that reads an empty CSV field as SQL NULL will silently make every `field = ''` test false — that bug produced a first round of wrong numbers in this very profile.

<!--
### D-n: <title> (YYYY-MM-DD, <name>)
Context: …
Decision: …
Alternatives considered: …
Consequences / measurements: …
-->
