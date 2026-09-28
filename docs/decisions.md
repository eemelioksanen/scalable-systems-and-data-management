# Decision log

Record every design decision here, with who made it, when, and why. The report's architecture section and each member's individual-contribution answers are written from this log.

Status is one of: **proposed**, **accepted**, **superseded (by D-n)**.

## Query semantics (week 1)

The Flink job (`flink-job/`) and the oracle (`oracle/`) must implement exactly these. Accepting a proposal requires EDA evidence (see `docs/ROADMAP.md` §3).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-1 | Symbol key | `ID` column as-is (e.g. `RDSA.NL`). `exchange` is derived from the suffix. | **accepted** | Confirmed: 11,784,671 rows / 5,493 symbols all match `^[A-Za-z0-9]+\.[A-Za-z]+$`, zero exceptions. Symbols may be fully numeric (`846741.ETR`), so never parse a key as a number. |
| D-2 | Price event | A row whose `Last` is non-empty **and > 0**. All other rows are quote or housekeeping events: counted, then dropped before `keyBy`. | **accepted (refined)** | 20.28% of rows are price events. 17,593 rows carry `Last` = `0.000000`; these are excluded, since feeding a zero into the EMA would drag the average down. The original wording ("non-null") would have included them. |
| D-3 | Event time | `Trading date` + `Trading time` parsed to epoch ms, with `Date` substituted when `Trading date` is absent (D-25). One timezone interpretation, used everywhere. | **accepted, timezone open** | `Trading time` is `HH:MM:SS.mmm` on 100% of price rows, so epoch ms is lossless (D-22). The spec says CEST, but every date in the dataset falls in CET (UTC+1), so the offset is still to be fixed; any whole-hour offset preserves window alignment, so it shifts published `windowEnd` values but not which trades share a window. |
| D-4 | Close | The price with the latest event time in the window. Ties broken by source position (D-23). | **accepted** | Load-bearing, not a formality: **11.06%** of consecutive per-symbol price pairs (257,149 / 2,325,956) run *backward* in `Trading time` within file order, so "latest event time" and "last to arrive" genuinely differ. 9,981 same-millisecond groups carry **differing** prices, so a tie-break is required for determinism. |
| D-5 | Window firing | Fire when the event-time watermark passes the window end. Bounded out-of-orderness with idleness. Late events go to a side output and are counted. | proposed | Bound still unset. Deliberately not derived from this sample: it needs a full day rather than 21.7% of one, and the unexplained `Time` − `Trading time` anomaly (p50 **−310 ms**, 87% negative) must be resolved first. |
| D-6 | Empty windows | No price event in a window: EMA unchanged, no output, no crossover check. | proposed | The carry-forward alternative is an ablation. |
| D-7 | EMA init | EMA₀ = 0 exactly as the spec says. | proposed | Causes a BUY in every symbol's first window. Flag it in the viz and report. |
| D-8 | Day boundaries | EMA state carries over across days and the weekend. | proposed | |
| D-9 | Subscriptions | EMAs are computed for all symbols. Results are emitted only for subscribed symbols, via a compacted `subscriptions` topic and broadcast state. | proposed | |
| D-10 | Missing or stale timestamps | Drop them, or fall back to `Date`+`Time`? | open | Measured: `Trading time` is never missing on a price row; `Trading date` is missing on **2,201,703 / 2,389,887 (92.1%)**, and where present **always equals `Date`** (0 disagreements). That evidences the D-25 fallback, but the policy stays open pending the fuller EDA. |

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

### D-29: CSV replayer (2026-09-28, Leo Immonen, issue #6)
Context: #6 needs preload, maximum-speed, real-time×k and subset replays of the day files into `ticks`. The records need ingest timestamps and their source position, and a consumer must be able to tell a complete finite replay from a partial one.
Decision ([replayer.md](replayer.md)):
- **Pacing clock:** live replay paces by the system `Date` + `Time`, as the minimum over the last 256 rows that have one, never moving backward. Replay speed changes only record timestamps, never values.
- **Source position:** every record carries a `src` header, `<file index>:<line number>`. The value stays the D-24 projection.
- **Order and failure:** one sending thread, an idempotent producer and `acks=all`. The first unacknowledged record stops the replay, which then reports `FAILED`.
- **Completion:** the replay ends with a JSON report of the status, every line's fate, and each partition's offset range. A consumer uses only a `COMPLETE` report. `ReplayVerifier` checks a report against Kafka and, independently, against the source files.
- **Compression:** lz4 in every mode. A preload also batches up to 1 MiB with `linger.ms=50`; live modes keep Kafka's batching defaults.
- **Retention:** `kafka-init` creates `ticks` with `retention.ms=-1`.
- **Unprojectable rows** (neither 39 fields nor 40 with an empty last one) are counted as `rejected` and not sent.

Alternatives considered:
- *Pacing by the raw `Time`, as a running maximum or a running median over 255 rows.* Measured on 08.11.2021, both break on the opening snapshot. The maximum reaches 18:30 at line 195. The median reaches the snapshot's 17:35 block by line 30,859. Either would send the rest of the day unpaced.
- *Pacing by `Trading time`.* It is empty on quote and housekeeping rows.
- *Source position recoverable only by re-reading the files.* That is deterministic but unverifiable from Kafka alone. The header costs 12.8 bytes per record, 20% of the payload bytes.
- *A completion marker in `ticks`.* The Flink parser would count it as malformed. A separate control topic was not needed while a report file is.
- *No compression, or zstd.* On 10 M rows: no compression 73.7 bytes per record at 1.29 M rec/s, lz4 17.2 at 1.25 M, zstd 10.9 at 0.92 M.

Consequences / measurements (full weekday 08.11.2021, 53,994,554 data rows):
- 0 rejected rows. The report's MD5 matches Zenodo's.
- The verifier matched all 53,994,554 records to an independent re-read of the source: 5,493 symbols, each in its hashed partition, in file order, with no offset gaps.
- 3 M rows replayed in `preload`, `max` and real time at 100×, 10× and 1× produced the same content digest. The paced runs achieved their rate to within 0.1%, with a p99 send lag of 10–21 ms. `Time` has whole-second resolution on 99.9% of those rows, so the replay sends once-a-second bursts of up to 8,709 rows; the lag is the time to send a burst.
- A dry run reads and projects 2.22 M rows/s. Preload throughput and disk use are in [replayer.md](replayer.md#disk).
- A full-week preload needs about 5.1 GB of Kafka log with lz4, or 22 GB without compression.

### D-26: Query 1 job follows the data contract (2026-09-28, Leo Immonen, issue #7)
Context: #16 merged before the data contract (#18). Its parser dropped every price row without a `Trading date`, which is 92.1% of them, and it accepted `Last` = 0.
Decision: the Flink parser now applies:
- **D-2 (refined):** `Last` ≤ 0 is not a price event. It is counted as `nonPositivePriceEvents`.
- **D-25:** an empty `Trading date` is replaced by `Date`. Those price events are also counted as `systemDateEvents`.
- **D-22:** event time is epoch ms. Any fraction digits beyond milliseconds are truncated. This replaces D-19's nanosecond clause; the tie-break stays the Kafka offset (D-23).

A price row is `missingTimestamp` only when `Trading time` is empty or neither date field is set.
Consequences:
- `GoldenFixturesTest` replays `fixtures/golden/{synthetic,real}.json` through the operator within the D-18 tolerance, and both sequences match.
- A real-format replay through Kafka reconciled all 9 records across the counters.
- D-25 is still only proposed. The oracle (#5) must apply the same rule, or the two will disagree on nearly every row.

### D-25: `Date` substitutes for a missing `Trading date` (2026-09-28)
Context: `Trading date` is absent on 92.1% of price rows in the profiled sample.
Decision: build the date part of event time from `Trading date` when present, otherwise from the system `Date`. The time part is always `Trading time`, which is never missing on a price row.
Alternatives considered: dropping rows without a `Trading date` would discard 92% of all trades. Using `Date` unconditionally was rejected only for lack of evidence, not merit — the two never disagree where both are present.
Consequences: **proposed**, not accepted; it is the concrete form D-10 will take if the fuller EDA agrees. The oracle and the Flink job must apply it identically or they diverge on nearly every row.

### D-24: `ticks` carries a projection, not the raw line (2026-09-28)
Context: D-13 says the replayer forwards "7 fields copied verbatim from the CSV", which reads as though the file had 7 columns.
Decision: wording clarified. The file has **39 named columns** (40 fields per row, see D-21). The replayer emits a **projection** of 7 of them — `ID, SecType, Date, Time, Last, Trading time, Trading date` — selected but not parsed, normalised or classified.
Consequences: clarifies D-13 without changing the wire format. Normalisation stays inside Flink, so it remains part of the system under test.
Confirms the limitation **D-19** records: with no `Ask`/`Bid` in the envelope the job cannot separate quote events from housekeeping events. Measured, they are 28.41% and 49.56% of rows respectively. If that split is needed in-stream rather than as an offline figure, the envelope has to carry `Ask` and `Bid`; otherwise it stays an EDA statistic.

### D-23: source position is the tie-break (2026-09-28)
Context: D-4 needs a deterministic order among trades sharing an event time.
Decision: tie-break on position in the source file. For the replayer and Flink that is the Kafka offset (key = symbol, so a symbol's events occupy one partition and offsets totally order them); for the oracle it is `(file date, line number)`. The close is `max by (event_time, source_position)`.
Evidence: 57,096 groups share a symbol and a millisecond; **9,981 of them disagree on price**, so max-by-event-time alone is not deterministic.
Consequences: the replayer must preserve per-symbol file order, and any ordering analysis must stay within one sample chunk, since source order is contiguous only inside a chunk.

### D-22: event time is epoch milliseconds, with no sub-millisecond companion (2026-09-28)
Context: we considered carrying an `eventTimeMicros` field so that a window's latest trade could be chosen even though Flink's timers are millisecond-based.
Decision: rejected as unnecessary. `Trading time` is `HH:MM:SS.mmm` — **three** fractional digits on 100% of price rows — so epoch ms is lossless and Flink's native timers carry the source's full resolution.
Alternatives considered: (a) an extra `eventTimeMicros` long, now dead weight; (b) microseconds as the event-time unit throughout, which would have broken `Time.minutes(5)`, Kafka's millisecond record timestamps and Flink's own watermark-lag metrics.
Consequences: supersedes the `HH:MM:SS.ssss` precision stated in `ROADMAP.md`. Ties still occur *within* a millisecond, which is why D-23 is still required.
Supersedes the close-selection clause of **D-19**, which keeps event time as epoch nanoseconds so that trades inside one millisecond stay ordered. The source carries only milliseconds, so those sub-millisecond digits are always zero and order nothing; the tie-break that does the work there is the Kafka offset, which D-19 already applies and D-23 formalises. Dropping to milliseconds costs no ordering information and removes a field from the hot path.

### D-21: CSV structural tolerances (2026-09-28)
Context: the real file departs from its own header in ways that reject every row if parsed strictly.
Decision: parsers must (a) accept **40** fields per data row — a trailing delimiter adds an unnamed empty field to 100% of rows — and drop the last one; (b) skip comment lines by the `#` prefix rather than by a fixed count, because one comment line sits **after** the header; (c) expect no quoting anywhere (0 rows contain a double quote); (d) treat an empty field as null; (e) tolerate CRLF on the 11 leading comment lines and LF everywhere else; (f) match columns by position, as several header names carry a leading space.
Evidence: 0 malformed rows out of 11,784,671 once these hold; 100% before.
Consequences: applies equally to the replayer, the oracle and the EDA. A tool that reads an empty CSV field as SQL NULL will silently make every `field = ''` test false — that bug produced a first round of wrong numbers in this very profile.

### D-20: Local stack verification and dependency corrections (2026-09-28, Emilia)

The repeatable checker uses a disposable Compose project, separate image tags and host ports, and synthetic Kafka topics.
Kafka keeps its existing temporary storage: stop/start retains records, while removing the container resets them.
Emilia confirmed these choices before implementation.

Runtime validation found that the producer's transitive SLF4J API 1.7.36 could not load its 2.0.20 provider.
Manage the API at the existing `slf4j.version` so logging works in the packaged jar.
The dev-container Docker feature also required `moby: false` because its Moby packages are unavailable on the current Debian trixie base.
D-11 and D-12 otherwise remain unchanged.

[Validation evidence](local-stack-validation.md) records the builds, Kafka and Flink roundtrip, scaling, metrics, storage lifecycle, and dev-container checks.

### D-19: Query 1 operator implementation (2026-09-28, Leo Immonen, issue #7)

Context: implementing Query 1 in Flink before the EDA (#4) and the oracle (#5) exist.

Decision:
- **Parsing:** records are parsed and classified in the Kafka deserializer. Only price events leave the source. Non-price, missing-timestamp and malformed records are counted there and dropped.
- **Close selection:** event time is kept as epoch **nanoseconds** for close selection, so trades within one millisecond are still ordered (D-4). Windows, timestamps and watermarks use milliseconds. Ties go to the higher Kafka offset (D-15).
- **Window state:** the operator keeps one close candidate per open window (`MapState`), not a single current window. Each window's timer sits on its last millisecond (`windowEnd - 1`). With a 0 ms watermark bound, the first event of the next window completes it, matching spec §3.1 (7).
- **Late events:** an event is late only if its window has already been evaluated. It then goes to the `late-ticks` side output and the `lateTicks` counter. Events behind the watermark whose window is still open are accepted.
- **Finite replay:** `--bounded` ends input at the startup offsets. End of input advances the watermark to the maximum, so the **last window is evaluated too**.
- **Provisional defaults** (CLI flags, to be set from #2 and #4): trading timestamps at offset `Z` (D-3), a 60 s out-of-orderness bound (D-5), and 30 s idleness.

Alternatives considered: the Window API. It is kept as a benchmark variant (ROADMAP §4).

Consequences / measurements:
- 47 unit and mini-cluster tests pass.
- A synthetic replay through Kafka matched the expected EMAs, and the counters reconciled with the input (5 price, 1 non-price, 1 missing timestamp, 1 malformed).
- Limitation: the 7-field `ticks` envelope has no bid/ask fields, so the job cannot tell quote events from housekeeping events. Only the EDA can.

<!--
### D-n: <title> (YYYY-MM-DD, <name>)
Context: …
Decision: …
Alternatives considered: …
Consequences / measurements: …
-->
