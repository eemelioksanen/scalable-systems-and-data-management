# Decision log

Record every design decision here, with who made it, when, and why. The report's architecture section and each member's individual-contribution answers are written from this log.

Status is one of: **proposed**, **accepted**, **superseded (by D-n)**.

## Query semantics (week 1)

The Flink job (`flink-job/`) and the oracle (`oracle/`) must implement exactly these. Accepting a proposal requires EDA evidence (see `docs/ROADMAP.md` §3).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-1 | Symbol key | `ID` column as-is (e.g. `RDSA.NL`). `exchange` is derived from the suffix. | **accepted** | Confirmed: 11,784,671 rows / 5,493 symbols all match `^[A-Za-z0-9]+\.[A-Za-z]+$`, zero exceptions. Symbols may be fully numeric (`846741.ETR`), so never parse a key as a number. |
| D-2 | Price event | A row whose `Last` is non-empty **and > 0**. All other rows are quote or housekeeping events: counted, then dropped before `keyBy`. | **accepted (refined)** | 20.28% of rows are price events. 17,593 rows carry `Last` = `0.000000`; these are excluded, since feeding a zero into the EMA would drag the average down. The original wording ("non-null") would have included them. |
| D-3 | Event time | `Trading date` + `Trading time`, using `Date` only when trading date is absent. Interpret recorded times at fixed UTC+2, literal CEST, with a configurable offset. | **accepted** | Emilia selected the literal course wording on 2026-09-28. This is an interpretation of the brief, not an inference from the November calendar. See D-29. |
| D-4 | Close | The price with the latest event time in the window. Ties broken by source position (D-23). | **accepted** | Load-bearing, not a formality: **11.06%** of consecutive per-symbol price pairs (257,149 / 2,325,956) run *backward* in `Trading time` within file order, so "latest event time" and "last to arrive" genuinely differ. 9,981 same-millisecond groups carry **differing** prices, so a tie-break is required for determinism. |
| D-5 | Window firing | Discard and count events for closed windows; flush remaining windows at finite-input completion. Evaluate bounded out-of-orderness and idleness settings separately. | **late/EOF policy accepted; bound and idleness proposed (D-38)** | Emilia selected the late/EOF policy on 2026-09-28. The bound targets zero changed closes; faster bounds are ablations. Monday alone cannot settle the full-week bound, and D-28 shows replay timing and idleness matter. See D-29. D-38 proposes the defaults: the all-admitting bound without idleness for a finite replay, 1 s and 30 s for a live job. |
| D-6 | Empty windows | No price event in a window: EMA unchanged, no output, no crossover check. | **accepted** | Emilia selected skip on 2026-09-28. The carry-forward alternative is an ablation (`--empty-windows carry-forward`, D-42). |
| D-7 | EMA init | EMA₀ = 0 exactly as the spec says. | **accepted** | Kept unchanged by D-29. Causes a BUY in every symbol's first window: all 4,840 of Monday's advisories (D-27). Flag it in the viz and report. |
| D-8 | Day boundaries | EMA state carries over across days and the weekend. | **accepted** | Emilia selected carry-over on 2026-09-28. |
| D-9 | Subscriptions | EMAs are computed for all symbols. Results are emitted only for subscribed symbols, via a compacted `subscriptions` topic and broadcast state. | **accepted** | Implemented as D-28 (PR #27). |
| D-10 | Missing or stale timestamps | Substitute system `Date` only for a missing `Trading date`. Exclude and count missing or unparseable event timestamps. Preserve valid explicit dates, including mismatches with system date. | **accepted; summary-row interpretation open** | Emilia selected this policy on 2026-09-28. Do not substitute system arrival time for a supplied trading timestamp. Wednesday's summary-like rows remain included under D-2; any future exclusion requires a separate decision. See D-29. |

## Stack and infrastructure

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-11 | Stream processor | Apache Flink 2.2.1 (Java 17) + Kafka 4.3.1 (KRaft). | accepted | Event-time windows, keyed state, horizontal scaling. The Kafka connector (`5.0.0-2.2`) is not yet released for Flink 2.3. |
| D-12 | Build | Maven runs inside Docker (multi-stage `Dockerfile`). The IDE runs in a dev container. No JDK on hosts. | accepted | |

## Interfaces

Accepted 2026-09-28. The exact formats are in [interfaces.md](interfaces.md).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-13 | `ticks` content | The replayer forwards every CSV row as 7 raw columns (`ID, SecType, Date, Time, Last, Trading time, Trading date`). All parsing and normalization happen in Flink. | accepted | Normalization belongs to the system and is described in the report. Early filtering in the replayer is a design comparison (`--early-filter`, D-42). |
| D-14 | `ticks` encoding | UTF-8 CSV string, verbatim fields, empty = null, no quoting. | accepted | Compared against a binary encoding as a design comparison (`--encoding binary`, D-42). |
| D-15 | `ticks` key and partitions | Key = symbol, default partitioner, 8 partitions locally. The record timestamp is the replayer's send time. | accepted | Revisit the partition count for the cloud runs. The Kafka offset gives per-symbol arrival order for D-4 ties. |
| D-16 | Output records | JSON. `ema-results`: `symbol, secType, windowEnd, close, ema38, ema100, maxIngestTs, emitTs`. `advisories`: `symbol, secType, windowEnd, type (BUY/SELL), ema38, ema100, maxIngestTs, emitTs`. | accepted | D-40 defines the latency reported from them. `secType` enables equity/index filtering in the viz. |
| D-17 | `subscriptions` | Key = trader ID. Value = JSON symbol list (`["*"]` = all). A tombstone removes the trader. Compacted topic. | accepted | Results are emitted once per symbol. Fan-out to traders happens in the viz backend. |
| D-18 | Correctness check | The oracle writes Parquet with the same field names. EMAs are joined on (symbol, windowEnd) with 1e-9 absolute tolerance and exact `close`. Advisories are compared as sets of (symbol, windowEnd, type), reporting precision and recall. | accepted | Flink runs with a `["*"]` subscription for the check. |

## Log

Add later decisions below, newest first.

### D-42: Design-comparison variants (2026-11-17, Emilia Repo, issue #23) — proposed
Context: the roadmap's design-space ablations (§4) need the alternatives as selectable variants before #11 can measure them. D-6, D-13, D-14, D-19 and D-35 each left one of them for later. The watermark bound and idleness have been switches since D-38.
Decision:
- **Switches, the baseline by default.** The job takes `--operator process|window`, `--empty-windows skip|carry-forward`, `--state-backend hashmap|rocksdb` and `--ticks-encoding csv|binary`; the replayer takes `--early-filter` and `--encoding csv|binary`. A job without them is the baseline. The job sets the state backend even when the flag is missing, to `hashmap`, Flink's own default, so the record is explicit.
- **Effective configuration.** The job publishes its resolved settings, defaults included, as global job parameters, which Flink's REST API serves at `/jobs/<id>/config`. A bench config's `variant` object picks the variant; `bench/run.py` passes the flags, records the variant in `effective`, reads the job's own record back after submission and fails the run if the two differ ([bench/README.md](../bench/README.md#variants-d-42)).
- **Window API.** Flink's window operator over 5-minute tumbling event-time windows, with an aggregate function that keeps D-4's close by the same rule as `CloseCandidate`, and a process window function that keeps the EMAs in the key's global state. The window operator is constructed directly instead of through `.window().aggregate()`, so that it ends event time at end of input as `EndOfInputProcessOperator` does (D-38). It keeps the name `q1-ema`, so the metrics and the harness's lookups stay as they are. It has no `lateTicks` counter and no `openWindows` gauge; late price events still go to `late-ticks`.
- **Early filtering.** The replayer drops the selected rows that have an `ID` but no `Last`, the quotes and housekeeping of D-2, and counts them as `filtered`. A row without an `ID` still reaches the job, which counts it as malformed. Every row with a `Last` still reaches the job, which classifies non-positive prices, malformed values and missing timestamps as before. The surviving records keep each symbol's order, so the offsets still break ties (D-23).
- **Binary `ticks`.** A version byte, then the 7 projected fields, each a tag and its payload: dates as days since 1970, times as milliseconds of the day, `Last` as a binary64 ([interfaces.md](interfaces.md#binary-value-d-42)). The replayer writes a number only where the job's parser reads the same number from the text. Every other field stays text, and the job parses it with its CSV rules, so a row is meant to get the same classification in both encodings. `ticks-binary.tsv` checks that on every fixture row, each outcome included. The job rejects a number no CSV field could hold, such as a time past midnight, as malformed. `fixtures/semantics/ticks-binary.tsv` holds every row the replayer sends from the semantics fixtures in both encodings; the replayer's, the job's and the export's tests read it. The output topics stay JSON.
- **Carry-forward.** A window without a price event repeats the symbol's previous close: the EMAs move toward it, a result is published and the crossover check runs on it. Only the gaps between two windows of a symbol are filled, when the later window fires. Filled windows therefore carry that window's `maxIngestTs`, and nothing follows a symbol's last window.
- **Semantics and references.** Each variant is compared with this reference, and checked against it on the hand-derived fixtures first (`VariantsTest`, `oracle/tests/test_semantics.py`):

  | Variant | Output against the baseline's | Reference |
  |---|---|---|
  | Window API | the same | the admit-all oracle output (D-30), or the oracle at the job's bound (D-38) |
  | RocksDB | the same | as above |
  | Binary `ticks` | the same | as above |
  | Early filtering | the same; the job's `nonPriceEvents` is 0 and the replay report's `filtered` holds those rows | as above, with `filtered` added to the job's non-price count |
  | Carry-forward | more windows, and different EMAs and advisories after any gap | the oracle with `--empty-windows carry-forward` |
  | Watermark bound and idleness | live runs reject late events (D-38) | the oracle at the job's bound (D-38) |

Alternatives considered:
- *Binary output records:* the issue asks for JSON against a binary representation, but `ticks` is CSV (D-14), the issue ties the binary variant to the replayer (#6), and #24 found the `ticks` source the busiest stage. Results and advisories are under 1% of the records, and the dashboard, the correctness export and the subscription tools read their JSON.
- *Avro or Protobuf for `ticks`:* a schema library and its dependency for 7 fields.
- *Parsing every field in the replayer and sending only typed values:* a row the job classifies as malformed would need its own encoding, which copies the job's rules into the replayer. The text fallback leaves the job's rules in charge.
- *Carry-forward by a timer on every window end:* it would publish empty windows as the watermark passes them, but at end of input the maximum watermark has no last window, and every symbol would publish every 5 minutes through nights and weekends.
- *A chained pass-through that ends event time before the window operator's `keyBy`:* when one source subtask finished while another was idle, it moved the window operator's watermark to the end and turned the idle subtask's later events late. `endInput` on the keyed operator runs only after all its inputs have ended.
- *A keyed EMA operator after the window operator:* one more shuffle than the baseline has.

Consequences / measurements ([evidence](validation/2026-11-17-variants/README.md)):
- On the semantics fixtures, carry-forward turns 61 windows into 250: SPARSE.ETR's 3 empty windows and DAY.NL's night of 186. Its advisories stay the same there, because both gaps repeat a close at or above every earlier one. A full week would publish every window from each symbol's first to its last, including nights and the weekend. Not measured here.
- Carried windows have no admission of their own, so the D-40 stages are not defined for them, and the harness reports no latency stages for a live carry-forward run.
- The filling is proportional to each gap. A gap comes from the data's own dates: the week's windows all lie between Monday 07:05 and Friday 22:40 UTC (D-27's evidence), so no symbol fills more than about 1,340 windows. Input with dates years apart would make the job and the oracle fill every window between them.
- The Window API variant subclasses Flink's internal `WindowOperator`, as `EndOfInputProcessOperator` subclasses `KeyedProcessOperator`, so a Flink upgrade can break either.
- `ReplayVerifier` verifies CSV replays only and reports a binary replay as unverifiable. The dashboard's feed clock reads `ticks` as CSV, so it shows no feed time during a binary replay.
- No performance is measured here; #11 does that.

### D-41: Advisories, markers and per-trader filtering on the dashboard (2026-11-10, Leo Immonen, issue #13) — proposed
Context: D-33 built the dashboard over `ema-results` and left `advisories`, the advisory feed and per-trader delivery to #13. D-17 makes fan-out to traders the visualization's job. The roadmap (§5) asks for BUY/SELL markers, a feed filterable by exchange, SecType and subscription, and a heatmap sorted by most recent crossover.
Decision ([viz/README.md](../viz/README.md)):
- **Published records only.** The backend reads `advisories` into the same store as `ema-results`, with the same 288-window span and replacement by (`symbol`, `windowEnd`). Markers, the feed and the crossover sort all come from these records; the UI derives no crossover from the EMAs, so what it shows is what the job decided, including a first advisory measured against unpublished history.
- **All advisories in the span go to every browser**, in the snapshot and then as deltas, and the browser filters them. The week has 11,355, mostly each symbol's first-window BUY, so this is far smaller than the heatmap itself.
- **Subscriptions.** The backend also reads `subscriptions`, with the job's parsing rules, and sends each trader's current list. The subscription filter shows the advisories and heatmap rows of the symbols in that list. It is the current list, not the list at each advisory's time: the topic is compacted, so a restarted reader can rebuild only the latest list per trader, as the job does.
- **Heatmap.** Sorted by the newest published advisory by default, with the D-33 sign-flip sort and an A–Z sort as options. An advisory's window is drawn in the darker line color.
- **First-window BUYs hidden by default.** Every symbol's first window gives a BUY only because both EMAs start at 0 (D-7): 4,970 of the week's 11,355 advisories. They are published and correct, so the chart draws them hollow, but the feed and the crossover sort skip them unless shown, since they would bury the breakouts the spec's §3.4 asks to emphasize. A first window has EMA38 / EMA100 = 101/39 whatever the close, so the record alone identifies it; on the oracle's week output this rule picks exactly the 4,970 first windows and none of the other 6,385 advisories.
- **Latency, not lag.** The feed shows each advisory's wall-clock latency from admission to publication (`emitTs − maxIngestTs`, D-40), which speaks to the spec's "immediately" and does not depend on the replay rate.
- **Clock bar.** The main time is *results as of*, the newest published `windowEnd`. A status replaces D-33's "results N min behind feed": *window open*, *waiting for the next window*, or *feed idle*. That gap mostly measured the open window that §3.1 (7) requires, and in simulated minutes it scales with the replay rate (30 s of idleness wait reads as an hour at 120×). Watermark lag stays in Grafana. The sampled feed time is kept, smaller.
- **Filters in the URL** (`?exchange=…&secType=…&trader=…&first=1&sort=…#symbol`), so a view can be linked and the report figures captured reproducibly.
- **End-to-end check.** A Node script applies the WebSocket messages with the frontend's own reducer and dumps the views; `correctness/run.sh dashboard` compares them with the topic export and the oracle.

Alternatives considered:
- *Attributing each advisory to the traders subscribed at its `emitTs`:* exact for a live session, but not reproducible after compaction or a restart, and clock skew between the job and the subscription writer would decide edge cases.
- *Filtering on the server per session:* needed only if advisories were numerous; it would add a request type and per-session state for no gain at this volume.
- *Checking the rendered page in a browser:* the views are drawn on canvases. The reducer's state is what they draw, so the check reads that, and screenshots are inspected by eye.

- *The first-window marker from the first window seen (D-33):* wrong for a symbol subscribed mid-run, whose first published window is not its first. The chart now marks it only where a published BUY sits on that window.

Consequences / measurements ([validation](validation/2026-11-10-dashboard/README.md)): a first day alone shows almost only first-window BUYs, because crossovers need converged EMAs, so the check replays Monday unpaced and Tuesday live at 120× over 13 symbols, changing three traders' subscriptions at 12:00. Both captures, one built from deltas through the run and one from a fresh snapshot, passed all 17 checks: 1,222 cells and points and 12 advisories equal to the topics and the oracle, with zero late events. The two first-window BUYs in the span were flagged and hidden, and the 12 latencies equal the records'. A symbol dropped by a tombstone lost its 16:55 SELL, and one added at 12:00 showed both later crossovers.

### D-40: Live latency from admission to output (2026-11-01, Emilia Repo, issue #37) — proposed
Context: D-16 left open which latency to report, and D-36 left open how to split `maxIngestTs` to `emitTs` into the watermark wait and the rest. D-38 proposes a 1 s bound and 30 s idleness for live jobs, and the job uses them by default. A first live run at 100x with the earlier 60 s bound took 1.0 s at p50 and 21.3 s at p99 from last admission to emission, mostly the watermark waiting on quiet partitions.
Decision ([bench/README.md](../bench/README.md#latency)):
- **Endpoints.** Latency runs from admission, the `ticks` record timestamp the replayer sets when it hands the window's last price event to its producer, to the output record's timestamp. Under the topics' `CreateTime` that is the sink producer's `send()` call. The export keeps each output record's timestamp type, so a `LogAppendTime` topic would show. Neither end is the broker's append.
- **Stages.** `windowWait` to the feed reaching the window's end, `boundWait` to the end plus the bound, `alignmentWait` to readiness, `queueing` to emission, `sinkHandoff` to the output timestamp. They add up to the total for each result.
- **Readiness is modelled, not observed.** It is when the watermark of interfaces.md would pass the window's end, computed from every price event's partition, admission and event time, read back from `ticks` after the run. The job is not instrumented, so its semantics and output stay as they are. Everything between admission and the job's watermark, including the model's own error, lands in `queueing`, which can be negative. A queueing split into broker, fetch, watermark emission and network is not identifiable from these timestamps.
- **Watermark lag** is read from `q1-ema`'s `currentInputWatermark` at each Prometheus scrape, with the scrape's own timestamp, against the event time the feed had admitted by then. The same samples check the model.
- **Interval.** A result counts when its last price event was admitted in the measurement interval and it was emitted before the drain began. The warm-up and the windows the drain fires are counted, not measured.

Alternatives considered:
- *Instrumenting the job to record, per window, the record that moved the watermark:* watermarks carry no provenance, and it would change the job that the correctness gate verified.
- *Readiness from the 5 s watermark gauge:* at 100x a 5 s sample spans 500 s of event time, more than a window.
- *Subtracting event time from wall-clock time,* as Flink's event-time lag metrics do: the events are from 2021, so the difference measures the replay's start, not the system.

Consequences / measurements ([live validation](validation/2026-11-01-latency/README.md), one 12-symbol Monday at 100x on a 7-CPU, 8-GiB Podman VM): 1,269 measured results, 117 warm-up exclusions and two drain exclusions; admission to sink send p50/p95/p99 501/3,630/30,491 ms. All five count checks pass, the 148,465 exported prices equal the source count, and latency is complete. There are 176 operator-watermark samples at 88 scrape times, zero negative queueing and zero unmodelled results. Queueing includes model error and ranges from 27 to 586 ms; alignment wait reaches the 30 s idleness timeout. Model-ahead event time is not near zero: its 5,248,119 ms outlier straddles a sparse-feed jump, 110 ms after the model advanced, and the observed watermark catches up by the next scrape. Raw event-time discrepancy must be interpreted with admission timing and scrape cadence, not treated as wall-clock processing delay. A separate warm ticks reread took 20.473 s, excluding sorting, writing and modelling. Live collection required the Kafka client's LZ4 decoder extra. These are single-run functional measurements, not comparative performance or broker-publication measurements.

### D-39: Query 1 on a full weekday against the oracle (2026-10-13, Leo Immonen, issue #21) — proposed
Context: #21 asks for one full weekday through the replayer and the job, compared with the oracle by the comparer (#20), with values unchanged by replay speed and by bounded reordering. It was written when the job's bound was 60 s in both modes. D-38 has since set the defaults by mode and compared the full week at them. Open were one weekday at every setting and speed, and reordering on a full day file: D-34 reordered 11 symbols. The runs are in [the evidence](validation/2026-10-13-weekday/README.md).
Decision:
- **Reference.** Monday 08 November needs 25 ms in the source-order model (D-27), so no supported setting may reject an event of it. Every run is compared with the admit-all oracle output at `+02:00`, and its `lateTicks` must be 0 and final.
- **Settings and speeds** (`bench/configs/weekday/`): preloads at a `--bounded` job's defaults, at 60 s with 30 s idleness (the defaults #21 names) and at a live job's 1 s with 30 s; `--bounded` without watermark flags; live replays at 1000×, 300×, 100× and 10× at the live defaults.
- **Reordering.** Monday rewritten by the injector with 1 s of disorder, run as a preload at the `--bounded` defaults and at 60 s, and live at 100× with the 1 s bound; and with 5 min of disorder, a whole window, at a 10 min bound. A run on a rewritten file is compared with the oracle run on that file and with the one on the original.
- **Unchanged means bit for bit.** Besides the gate's 1e-9, each run's results and advisories are hashed over the D-18 fields. Runs with equal digests published the same closes and EMAs.
- **`lateTicks` of a `--bounded` job** is the sum of each q1-ema subtask's last sample from the TaskManager's metrics endpoint. It is final when those samples add up to every price event received.

Alternatives considered:
- *Only the `--bounded` defaults:* their 17.85 h bound outlasts the day, so the watermark passes no window of a one-day input and every window is evaluated at end of input. That run checks parsing, closes, EMAs and end of input, but not event-time firing.
- *The last scrape of a bounded job for `lateTicks`:* the subtasks finish at different times, and one scrape held three of the four.
- *1×:* a paced Monday takes 13.4 h.

Consequences / measurements (2026-10-13, Intel i7-11700K with 16 threads and 62.5 GiB, Docker 29.8.2 on Linux; `af7ad16`; 8 partitions, parallelism 4, one `*` subscription; single runs; two stacks shared the host, so nothing here is a performance result):
- **All 12 runs passed the gate:** 373,126 results and 4,840 advisories, `lateTicks` 0, no difference of any class, no duplicates, complete exports. The 11 harness runs reconciled 53,994,554 rows and 11,794,520 price events, with the source's counters equal to the oracle's.
- **Bit for bit.** Every run has the oracle's digests. Replay speed, the watermark settings and both reorderings changed no close, EMA or advisory, including the 50 windows where binary64 drifts more than 1e-9 from exact (D-27).
- **Event-time firing.** At 60 s all but 180 results were published as the watermark passed, and at 1 s all but 97, in the preloads and live from 100× to 1000×. With 5 min of disorder and a 10 min bound, 366 were left for the end of input.
- **Reordered files.** The oracle's output is the same for all three files. With seed 1, 1 s of disorder needs a 981 ms bound and 5 min needs 298,801 ms. The live default of 1 s admitted the first at 100×.
- **Live runs.** Each replay started 10.4 to 11.2 s after its job, inside the idleness timeout (D-38). At 1000× the replayer achieved 944× and fell up to 19.6 s behind on the shared host. At 10×, where 30 s of idleness is 5 min of feed time, partitions went idle in the day's last minutes and released 90 of those 97 results before the input ended. No run rejected an event.
- **Control.** Against an oracle run at a 0 ms bound, which rejects 79 events, the gate refuses the reference and reports 38 differing closes and 2,894 later EMAs in 35 symbols, all as `lateRejection`.
- **Not covered:** Monday's advisories are all the first-window BUY of D-7, so no SELL or later crossover is checked here. No event is late, so rejection is not exercised. Restart, subscriptions and the weekend files are #9's.
- `correctness/README.md` gave 923 ms for 1 s of disorder on Monday without naming a seed. Seeds 0 and 1 give 941 ms and 981 ms, so it now cites seed 1.

### D-38: Watermark bound, idleness and end of input (2026-10-13, Leo Immonen, issue #7) — proposed
Context: D-5 left the bound and the idleness timeout open, and D-32 found no setting that is both correct by D-30 and real-time. One burst causes the conflict. On 10 November, 5,506 summary rows arrive at 18:56 with `Trading time` 01:00. Under admit-all they open a 01:05 window for 1,812 indices, which changes every later EMA of those symbols. Admit-all's results for that Wednesday therefore depend on rows that arrive at its end, so no job can publish them during the day and also match admit-all. A bound can only choose between waiting 17.85 h and publishing without the burst. D-30's source-order model also left the runtime open: replay speed, preloads and idleness. The runs behind this entry are in [the evidence](validation/2026-10-13-watermark/README.md).
Decision:
- **Defaults by mode.** A finite replay (`--bounded`) defaults to a 64,265,001 ms bound with idleness off. It admits every event of the week, so it is the admit-all reference of D-30, and its output does not depend on how fast it reads. A live job defaults to a 1 s bound with 30 s idleness. It publishes a window about a second of event time after the next one starts, and rejects the burst. Either flag still replaces its own default in both modes.
- **Live bound, 1 s.** Outside the burst the week needs 264 ms in every partition layout from 1 to 32 (D-30). The rest is margin. A longer bound buys nothing on this data: 60 s rejects the same 5,506 rows and publishes every result 59 s later at 1×.
- **Live idleness, 30 s.** A partition without a price event for 30 s of wall-clock time stops holding the watermark back. Without idleness an empty partition blocks every result, and a quiet one delays them all. 5 s lost ticks at the start of a replay (below). 30 s is longer than the 11 to 21 s the harness took from starting the job to starting the replay.
- **End of input.** `q1-ema` ends event time itself when its input ends, on a bounded input and on `flink stop --drain`. Flink leaves that to the sources' final watermark, and drops it for a source subtask that is idle at that moment.
- **References.** The correctness gate (#9) runs a finite replay against the admit-all oracle output and requires `lateTicks` 0. A live run is compared with an oracle run at the job's bound, and its `lateTicks` must equal that run's `lateRejected`: 5,506 for the week at any bound from 264 ms to about 17.8 h.
- **Effect of late events.** A late event is counted in `lateTicks`, sent to the `late-ticks` side output and left out of the calculation. If it would have been its window's last event, the window closes on an earlier price. If it would have been the only one, the window is never published. Either way the symbol's later EMAs and crossovers differ from a run that admitted it. For the week's burst that is 1,812 missing windows, 684,661 changed EMA windows, and 422 advisories removed and 416 added, out of 1,857,457 windows and 11,355 advisories (D-30). Outside the burst no bound of 264 ms or more changes a close.

Alternatives considered:
- *64,265,001 ms in both modes:* correct by D-30, but every live result then waits 17.85 h of event time: 10.7 min at 100× and most of a day at 1×. The dashboard and the latency runs (#37) would show nothing else.
- *One short bound in both modes, as before:* a preload admits or rejects the burst depending on how far its watermark trails its reads. Three drained full-week preloads at 60 s rejected 5,506, 5,506 and 4,044 rows, so the same input gave two different outputs. D-28 and D-34 saw 0 rejected on smaller inputs.
- *Publishing on time and correcting later:* needs every symbol's closes kept for 17.85 h, up to 684,661 results sent again, and advisories withdrawn after traders have seen them. D-16's outputs are append-only.
- *Dropping the burst in the parser:* D-10 and D-30 keep those rows as price events, and the exact summary signature matches only 5,492 of them.
- *Idleness of 5 s or 1 s:* on 12 symbols at 100× it cut the p99 from a window's last price event to its result from 30.4 s to 5.4 s and 2.4 s. But every partition is then idle before the replay starts, so the unpaced rows before `--pace-from` race each other. With every symbol, 5 s rejected 600 ticks beyond the burst, which removed 30 windows of Monday morning and changed 3,990 later EMAs in 10 symbols.
- *No watermark before the end of a finite replay:* admits any input, not only this week's, but holds all 1.86 M windows open at once and fires them in one burst, which is not the job the throughput runs measure.
- *A feed clock instead of idleness (heartbeats from the replayer, or a processing-time flush):* would also publish the day's last windows on time, but changes the `ticks` contract (D-13) and adds a second clock to event time.

Consequences / measurements (2026-10-13, Intel i7-11700K with 16 threads and 62.5 GiB, Docker 29.8.2 on Linux; 8 partitions, one `*` subscription; up to three stacks shared the host, so nothing here is a performance result; single runs unless stated):
- **Finite replay reference.** The full week preloaded and drained at 64,265,001 ms with idleness off: `lateTicks` 0, and all 1,857,457 results and 11,355 advisories equal the admit-all oracle output, before and after the job change. A `--bounded` run with no watermark flags gave the same. This is the first full-week comparison: D-37's preloads matched only the oracle's counts. Up to 623,030 windows were open at once, summing each subtask's highest sample.
- **Short bounds in a preload** never matched admit-all, in six runs at 60 s and 1 s: 686,473 EMA differences and 838 advisory differences in the five that rejected the whole burst.
- **Live, every symbol,** Monday to Wednesday paced from Monday 09:00, through two nights: at 300× with 60 s and with 1 s, and at 100× with 1 s, the job rejected exactly 5,506 events, and its 1,116,608 results and 6,607 advisories equal the oracle's at that bound. The nights lost nothing: D-28's loss needs unpaced input after the gap.
- **Live, 12 symbols on 6 of the 8 partitions,** Monday from 09:00 at 100× with 30 s idleness: 0 late and equal to the oracle, at 60 s and at 1 s. From a window's last price event entering Kafka to its result took 1.0 s at p50 with 60 s and 0.44 s with 1 s, and 30.4 s at p99 with both. The tail is the idleness timeout, as #10 found.
- **Windows lost at end of input, now fixed.** Before the change, the 12-symbol runs with 5 s and 1 s idleness published 1,384 to 1,386 of 1,388 windows with `lateTicks` 0, because the harness drained them after the sources had gone idle. The 30 s runs escaped only because their drain came 9 to 16 s after the replay ended. The same 5 s run on the changed job published all 1,388. `TradingTrendsJobTest` reproduces the loss on a bounded input whose source is idle.
- **Rule for live runs.** Input must not arrive unpaced after the job has gone longer than the idleness timeout without any. Every partition is then idle, and the first to deliver advances the watermark alone (D-28). So start a replay within 30 s of the job. A replayer that stalls for longer than the timeout and then catches up has the same shape, though the one 36 s stall here lost nothing. The late count shows a violation, and the comparison then fails.
- **The day's last windows wait for the next event.** Once every partition is idle the watermark stops, so the windows after the feed's last price event are published when it resumes, or at end of input: 102 s later at 300×, about 8.5 h at 1×.
- **For #37:** a window ending at W is published once every partition that is not idle has read an event at or after W plus the bound. At replay rate k the bound costs 1 s / k of wall-clock time. Idleness is wall-clock and does not scale with k.
- **Left as they are:** the live configs in `bench/configs/baseline/` and `subset12-live-100x.json` state 60 s and 30 s, the settings of the runs they record. #11 should run its live configs at 1 s. The job still does not export its late events (#38).
- The Kafka image did not start under Docker's BuildKit, which gave the JMX agent's directory mode 0644. The Dockerfile now sets 0755.

### D-37: Local baselines, run procedure and partition counts (2026-10-12, Emilia Repo, issue #24) — proposed
Context: #24 runs each planned configuration once on the local VM to find the bottlenecks and to fix the matrix, the partition counts and the procedure that #11's and #12's repeated runs use. The runs are tuning data ([evidence](validation/2026-10-12-baselines/README.md)); #9 still precedes any published performance conclusion.
Decision:
- **Procedure:** keep D-36's full reset before every repeat, so each starts with cold JVMs and an empty Kafka. Throughput stays the `ticks` source's records over its subtasks' span, from Flink's record of a bounded preload, ramp included. Compare throughput only between jobs of similar length: the later scrapes read 2 to 3% faster than the whole job at p1 and p2 (42 to 59 s jobs), 8 to 10% faster at p4 (36 s) and 12 to 14% faster in the 20 to 26 s exchange-subset jobs. So #11 compares exchange subsets by job duration as well as by rate, and reports the post-warm-up read rate from the Prometheus summary next to the whole-job rate. `warmupS`, which affects only the Prometheus summaries, is 15 for preloads and 25 for live runs. 25 s keeps every 15 s rate window clear of the unpaced rows before `--pace-from`. No warm-up gives a short preload a clean steady state, so preload summaries stay approximate. Neither value has been run yet; #11's first repeat checks them.
- **Local matrix for #11** (7 vCPUs, 8 GiB): the stacks of `bench/configs/baseline/`: parallelism 1, 2 and 4 on 8 partitions; FR, FR with NL, and all exchanges at p4; live replay of 08 November from 09:00 at 100x, 300x and 1000x, and 3 M-row slices at 1x and 10x. Parallelism above 4, several TaskManagers and other partition counts are dropped locally: they ran no faster, and the VM's CPU hides what they would show.
- **Partitions for #12:** as many as the largest parallelism #12 runs, with parallelisms that divide it (16 partitions for p1 to p16). Then the record counts per subtask do not depend on the partition count. Check any other choice with `bench/predict_load.py`.
- **Input rate:** one replayer offers at most about 1.5 M records/s. A rate experiment that has to saturate the job needs several replayers. Nothing builds them yet; #12 has to add them or drop that experiment.

Alternatives considered:
- *Reusing a preloaded topic across repeats and resetting only Flink:* saves the 3 min preload per repeat, but each repeat would no longer record its own preload rate, and the output topics and Prometheus history would need their own resets. At about 6 min per full-week run, #11's matrix with 3 repeats takes about 3 h, which is affordable.
- *Steady-state throughput from Prometheus as the headline:* excludes the ramp, but rests on one to nine scrapes per run.
- *More partitions against skew:* with a dividing parallelism they change no subtask's records. Keying `ticks` by something other than the symbol, or partitioning round-robin, would balance the source, since `keyBy` regroups by symbol anyway. It would lose each symbol's single partition, whose offsets give the arrival order that breaks ties (D-23). Hot-key-aware partitioning, the roadmap's optional item, keeps it.

Consequences / measurements (2026-10-12, Apple M4 Pro, rootless Podman VM with 7 vCPUs and 8 GiB; 15 single runs; full week, 289,116,808 rows, unless stated):
- The job read 4.93 M records/s at p1, 6.87 M at p2 and 8.05 M at p4. p6 and p8 read 6.93 M and 7.49 M, and p4 on two TaskManagers 8.08 M. From p4 the VM held at 5.8 to 6.4 of its 7 cores, up to 5.7 in the TaskManager, so the VM's CPU is the likeliest limit. No profile was taken.
- No task thread was saturated. The `ticks` source was the busiest stage (busy 0.69 to 0.85) and its aggregate idle time was 16 to 25% from p4, waiting on its Kafka fetchers, whose CPU no busy metric counts. `q1-ema` was busy 0.34 to 0.52, the output 0.08 to 0.13.
- While the job read a preload, the broker used at most 0.55 cores with its request handlers at least 99% idle. The replayer preloaded 1.44 to 1.59 M records/s on 1.06 to 1.21 cores, against 3.32 M rows/s when it only reads and projects, with the VM mostly idle: its one sending thread most likely limits ingestion.
- `bench/predict_load.py` matched every source and `q1-ema` subtask's records exactly in all ten preloads, including p6 on 8 and 16 partitions (source skew 1.72 and 1.33 in records, 1.39 and 1.21 in busy time) and the FR and FR with NL subsets. Predicted skew at p16 on 16 partitions: 1.49 in the source, 1.27 in `q1-ema`.
- Live replay of 08 November at 100x, 300x and 1000x: the job kept up at every rate, with no lag after the first scrape beyond 259 records, and exported the oracle's 373,126 results and 4,840 advisories with `lateTicks` 0. At 1000x the replayer reached 1.51 M records/s and fell up to 4.8 s behind its schedule.
- p4 on 16 partitions read 7.26 M records/s against 8.05 M on 8 with identical record counts. One run each cannot separate that from run-to-run variation.

### D-36: Benchmark harness (2026-10-12, Emilia Repo, issue #10) — proposed
Context: #10 needs scripted runs (reset, replay, warm-up, measurement, completion) with stable run IDs, retained raw results and configurations, mean and std over repeats, count reconciliation and the correctness check (#20). #24, #11 and #12 run on it. The live-latency criteria depend on the watermark bound and idleness that #7 has not chosen.
Decision ([bench/README.md](../bench/README.md)):
- **Runs:** `bench/run.py` takes a JSON config and repeats it. Each repeat resets the stack with `docker compose down --volumes`, writes the subscriptions, then either preloads and submits the job, or submits the job and replays live. The run ID is `<name>-<UTC stamp>-r<k>`, and the replay report carries it. The config must state the watermark bound and idleness; the harness has no defaults for them.
- **Completion:** a bounded job ends itself. A drained job ends once the sum of its Kafka offsets reaches the replay's end offsets and q1-ema has received every price event; `flink stop --drain` then fires the remaining windows, as end of input does in a bounded run. Live runs always drain.
- **Collection before reset:** Flink's REST record of the job, every D-35 query as a 5 s range query, the counters' last samples, the effective Compose file, container limits and image IDs, hardware, the code revision and input MD5s, and the correctness export. Preload time comes from the replayer's report, not from Prometheus.
- **Reconciliation** uses the per-vertex record counts Flink keeps after a job ends: replayed records = records read, price events out of the source = into q1-ema, records written = records exported, and windows evaluated = `ema-results` exported under a `*` subscription. A counter from Prometheus is marked final only when its last sample agrees with those counts, and `compare` gets `lateTicks` only when it is final. A run that fails reconciliation or the comparison is `INVALID` and stays out of the means; one whose only gap is an unknown `lateTicks` is `unverified`.
- **Throughput:** records the `ticks` source read over the span of its subtasks, from Flink's record of a bounded job, which leaves out scheduling. Checkpointing needs a drained run, since a bounded job would wait for a final checkpoint.
- **Latency:** every export keeps each result's `maxIngestTs` and `emitTs`. The harness computes no latency from them until #7 settles the bound and idleness.

Alternatives considered:
- *Counters from Prometheus alone:* a bounded job's metrics vanish when it finishes, and a 13.8 M-record preload ran in 3.5 s, between two scrapes.
- *Polling the TaskManagers' metrics endpoint during a bounded run:* a source finishes and unregisters its metrics right after its last record, so the last poll can still miss increments.
- *Draining every run:* exact counters, but the source's tasks then run on until the harness notices the job is done, so throughput runs keep `--bounded`.
- *A lighter reset (recreating topics):* faster, but keeps Flink's JVMs warm and Prometheus's history between repeats.

Consequences / measurements (2026-10-12, rootless Podman VM with 7 CPUs and 8 GiB, 8 partitions, one TaskManager with 2 slots; 12 symbols of 08 to 10 November, 13,810,579 rows, admit-all oracle; 10 runs at one revision, [evidence](validation/2026-10-12-bench/)). These are tuning data, not results (#9):
- Bounded preloads, three at parallelism 1 and three at 2: every run reconciled exactly (13,810,579 read, 492,434 price events, 4,166 windows, 17 advisories) and had 0 EMA and 0 advisory differences. All six are `unverified`: `lateTicks` was never scraped. The job read 3.96 ± 0.10 M records/s at parallelism 1 and 4.24 ± 0.25 M at 2, over source spans of about 3.3 s, and the replayer preloaded 1.60 ± 0.03 M records/s. With 12 symbols one source subtask reads 10.8 M records and the other 3.0 M (skew 1.56), so these say nothing about scaling.
- Three drained preloads: the source's counters matched the oracle's input split exactly (492,434 price, 13,318,109 non-price, 36 non-positive, 182,540 dated from `Date`), `lateTicks` was final at 0, and the gate passed every time.
- Live replay of 1.5 M rows of 08 November at 100× from 09:00, with the provisional 60 s bound and 30 s idleness: pacing achieved 100.0×, p99 send lag 7 ms; all counts reconciled and `lateTicks` was 0.
- Failure handling, checked by hand: a job that failed at startup, and one cancelled during a live replay, each stopped the run within seconds with status `FAILED`, kept Flink's record of the job and the metrics, and stopped the replayer.
- **Open, for #7:** separating the watermark wait from queueing and processing needs the time the watermark passed each window's end. Flink's watermark gauges are sampled every 5 s, so per window it would have to be modelled from `ticks` (record timestamps, event times, partitions) under the run's bound and idleness. Watermark lag needs the replay's source clock against the job's watermark. Both depend on the bound and idleness #7 chooses.

### D-35: Kafka, container and host metrics (2026-10-12, Emilia Repo, issue #22) — proposed
Context: the benchmark harness (#10) and the bottleneck runs (#24) need throughput, lag, backpressure, CPU, memory, GC, network and state per subtask from Prometheus, which scraped only Flink.
Decision ([queries in interfaces.md](interfaces.md#metrics-d-35)):
- **Kafka:** the Prometheus JMX exporter agent runs in the broker, from a `kafka` image target that adds its jar by checksum. Its rules keep messages and bytes per topic, each partition's end offset, and the request handler and network idle ratios; the agent adds the broker's JVM. Compose passes the agent as a start argument, so the Kafka tools in the healthcheck and in `docker compose exec` do not start a second agent on the same port.
- **Containers and host:** cAdvisor, limited to CPU, memory and network. It runs privileged in the host's cgroup namespace and mounts the Docker host's `/`, `/sys`, `/var/run` and `/var/lib/docker` read-only.
- **Lag:** Flink's `pendingRecords`, cross-checked against the broker's end offsets. Without checkpointing the job commits no offsets (D-28), so the broker has no consumer-group lag to report.
- **State:** two `q1-ema` gauges count state entries per subtask: symbols with EMAs and open windows. Heap state has no size metric, so bytes come from checkpoint sizes in a run submitted with checkpointing.

Alternatives considered:
- *A separate Kafka exporter reading consumer groups:* the job commits no offsets, and the JMX rules already give the topic offsets.
- *node-exporter for the host:* cAdvisor's root cgroup already gives the host's CPU, memory and network.
- *Checkpointing on by default for state bytes:* changes D-28's failure behavior and puts checkpoint barriers into every measured run.
- *RocksDB native metrics:* bytes per subtask, but only with the RocksDB backend, which is an ablation.

Consequences / measurements (2026-10-12, rootless Podman 6.1.3 VM with 7 CPUs and 8 GiB; defaults of 8 partitions, 2 slots and parallelism 2; [evidence](validation/2026-10-12-metrics-under-load.json)):
- Flink's `busyTimeMsPerSecond` and `backPressuredTimeMsPerSecond` average over 60 s and read busy as 1000 until a task's first update, so a run shorter than a minute shows nothing reliable in them. The queries use the rates of the accumulated idle and backpressured times instead, which follow a run within about 5 s.
- **Two-day backlog.** All symbols of 08 and 09 November, 112,806,424 records, were preloaded in 71 s at up to 1.63 M records/s (29.1 MB/s) with the job stopped. The job, submitted afterwards, read them at 4.1 to 4.8 M records/s per source subtask.
  - Lag at the first scrape was 89.2 M records by the broker's offsets and 89.1 M by `pendingRecords`. It was 0 ten seconds later, so the busy and backpressure figures below rest on two scrapes.
  - The ticks sources were busy about 800 to 860 ms/s and backpressured 25 to 38 ms/s; `q1-ema` was busy about 480 to 520 ms/s. So the sources were the busiest stage, and `q1-ema` sometimes could not take their output at once. The TaskManager used up to 4.3 of the VM's 7 cores.
  - All symbols spread evenly. The source subtasks read 56,652,816 and 56,153,608 records, `q1-ema` received 11,652,713 and 12,041,928 price events, and `emaStates` reached 2,493 and 2,423.
  - `openWindows` peaked at 48,320 and 48,597 while the watermark trailed the backlog (D-28), then fell to 4 and 10, the windows still waiting for the watermark. With 10 s checkpoints, `q1-ema` held 78,891 and 77,140 bytes of state, about 32 bytes per symbol, and the whole job 169,284 bytes.
  - Working set at its highest: Kafka 1.35 GB, the producer 1.11 GB, the TaskManager 945 MB. Kafka's cgroup peak, page cache included, was 2.85 GB. The VM's working set reached 5.37 GB of 8 GiB.
  - GC: the TaskManager up to 24 ms/s (a 60 s average), the broker up to 0.8 ms/s. Network: the TaskManager received up to 169 MB/s, and Kafka sent up to 183 MB/s.
- **Twelve symbols, preloaded while the job ran.** 13,810,579 records of 12 symbols, 08 to 10 November, reached Kafka at up to 1.25 M records/s. The job kept up: no scrape showed any lag.
  - The 12 symbols hash to 6 of the 8 partitions. Source subtask 0 read 10,778,797 records and subtask 1 read 3,031,782; `q1-ema` received 213,460 and 278,974 price events for 5 and 7 symbols. With few symbols the partition hash decides each subtask's load, which matters for #24's subset runs.
- Under rootless Podman, cAdvisor needs the Podman socket instead of Docker's and no `/var/lib/docker`, and it reports each container under three cgroups; `max by (name)` in the queries picks the container. Docker Desktop and Docker on Linux were not run.

### D-34: Correctness tools (2026-10-12, Emilia Repo, issue #20) — proposed
Context: the one-weekday check (#21) and the full-week gate (#9) need to export the job's output, compare it with the oracle without hiding missing, duplicate or rejected results, and feed the job disordered and late input whose expected output is known.
Decision ([correctness/README.md](../correctness/README.md)):
- **Export:** read each output topic up to the end offsets taken at the start, and record them per partition. An export is complete only if every partition starts at offset 0 and holds one record per offset. The export records the job's zone offset, which must equal the oracle's.
- **Classification:** every difference is `lateRejection` (either side rejected an event of the symbol in that window or earlier), `precisionDrift` (same close, EMAs within 66 ulps, twice D-27's measured drift), `pipelineLoss` (missing from the job, unexplained) or `unexplained`. All classes fail the gate; D-18 is unchanged. Whether an unexplained difference is an oracle error needs a person to check the input at the oracle's close position.
- **Late counts:** the gate also fails unless the job's `lateTicks` equals the oracle's `excluded` + `lateRejected`, so a run is not judged against a reference that rejected a different number of events. Equal counts do not prove the same events were rejected; the job's late-event list (D-31) would.
- **Disorder:** a price row's sort key is the running maximum event time plus a seeded delay in [0, B], never below an earlier row with the same symbol and event time. Every event then needs at most B more bound in the oracle's admission model, and closes are unchanged.
- **Late events:** a chosen price row is held until the written rows reach its window end plus L, and is listed with the bound it needs.

Alternatives considered:
- *Shuffling rows within a fixed number of positions:* bounds displacement in rows rather than in event time, so it says nothing about the watermark bound a run needs.
- *Deciding "oracle error" automatically:* both implementations follow the same written rules, so a difference beyond drift cannot be blamed on either side without reading the input.

Consequences / measurements (2026-10-12, local stack, 11 symbols of 08-10 November, 12,722,292 records, bound 64,265,001 ms, idleness off, `--bounded` preload):
- Original input: 3,720 windows and 23 advisories (15 BUY, 8 SELL) equal to the admit-all oracle, `lateTicks` 0. The gate passes.
- With 1 s of injected disorder: the same 3,720 windows and 23 advisories as the original's oracle output. The gate passes.
- With 161 injected late events at 60 s: the job counted 0 late events and the 60 s oracle run 170, so the gate refuses that reference. All 507 differences are `lateRejection` by the oracle. Against an admit-all oracle run on the same files the gate passes. A preload lets the watermark trail the reads (D-28), so testing the job's own late rejections needs a paced replay.
- The job still only counts its late events (D-31). Until it exports them, `--job-late-events` has no input from a real run.

### D-33: Dashboard over `ema-results` (2026-10-11, Eemeli Oksanen) — proposed
Context: #26 asks for the part of the roadmap's dashboard that needs only `ema-results`, without waiting for #13. The design is in [the spec](superpowers/specs/2026-10-11-dashboard-ema-results-design.md).
Decision:
- `viz/` is a Java service (kafka-clients, Javalin) that serves a React + Vite frontend and one WebSocket. It reads `ema-results` from the earliest offset without a consumer group, so a restart rebuilds its state from the topic.
- It keeps the last 288 five-minute windows (24 h of simulated time) of every published symbol, ending at the newest `windowEnd` seen. Older results are dropped. Memory is bounded by symbols × 288 in the backend and the browser, however long a replay runs.
- The replay clock shows two simulated times next to wall-clock time: the feed time from the tail of `ticks`, read with the replayer's source-clock rule, and the latest evaluated window. The gap between them shows the watermark delay. (D-41 replaces the gap with a status: in simulated minutes it scales with the replay rate.)
- The heatmap shows every published symbol, sorted by its latest sign flip of EMA38 − EMA100. A symbol's first window counts as a flip, matching the BUY that zero initialization gives every symbol (D-7).
- Prices carry no currency label: no topic carries the currency (D-24, D-16), and the chart says so.

Consequences:
- After a preload the feed time shows the end of the input at once. It is only meaningful during a live replay.
- #13 adds `advisories` as another consumer feeding the same store, and extra fields in the WebSocket messages (D-41).

### D-32: UTC+2 runtime default and status alignment (2026-10-08, Leo Immonen)
Context: D-3 accepted fixed UTC+2 on 2026-09-28, but the Flink job and the oracle still defaulted to `Z`, D-19's provisional value, while the analysis already used `+02:00`. D-29 and D-30 left the change to separate implementation work. The README and the oracle README also still described the pre-D-30 correctness reference, and the docs put the day files in two different places.
Decision:
- `--zone-offset` defaults to `+02:00` in both the job and the oracle. Window contents do not change, since any whole-hour offset keeps the windows aligned to local midnight. Only the epoch values move: `windowEnd` and event times are 7,200,000 ms smaller than under `Z`.
- The recorded oracle evidence and the hand-derived fixtures stay at `Z`. Their tests pass `Z` explicitly, and [the evidence README](../oracle/evidence/README.md) says how to reproduce them.
- The day files live in `data/daily/` in every document, where `analysis/src/fetch_day.py` downloads them.
- The oracle README's comparison steps follow D-30: admit-all is the baseline reference, and a bounded run is an ablation compared with its matching oracle run.
- D-7 and D-9 are marked accepted: D-29 kept the zero start, and D-28 implemented the subscriptions.

Consequences:
- Output written before this change compares with new output only after shifting `windowEnd` by 7,200,000 ms.
- The bound and idleness stay provisional (D-5). The 60 s default does not meet D-30's admit-all target, which only a bound of about 17.85 h is shown to meet, and the 30 s idleness lost ticks in a live replay (D-28). The baseline therefore has no runtime setting yet that is both correct by D-30 and real-time; this needs a team decision before the correctness gate (#9).
- The job counts its late events but does not export them or read the `src` header. A run whose late count is neither 0 nor the model's therefore cannot yet be checked against the oracle ([oracle README](../oracle/README.md#late-events-matching-a-batch-run-to-a-streaming-run)).

### D-31: CSV replayer (2026-09-28, Leo Immonen, issue #6)
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

### D-30: Full-week analysis evidence (2026-09-29, Emilia Repo, issue #4)

The [full-week report](eda-week.md) processes all seven files under D-29 and verifies complete admit-all and single-partition 60-second outputs against the recorded oracle hashes.
The accepted correctness target remains zero missing or changed closes against admit-all.
A 64,265,001 ms bound admits every event in the tested source-order models with 1–32 partitions; this is sufficient, not a proven minimum preserving closes or a runtime default.
Bounds of 264 ms and 60 seconds reject 5,506 Wednesday prices and lose 1,812 windows.
In the eight-partition model they also change 684,661 existing EMA windows, remove 422 advisories and add 416.
They remain explicit faster ablations.

The exact same-row open/high/low signature matches only 5,492 of the 5,506 index rows at 01:00.
Their per-symbol counts are not uniformly three: 1,740 symbols have three rows, 71 have four and one has two.
Both lists are retained in the report; excluding the narrower signature is a separate experiment, not an accepted parser rule.
All supplied timestamps and positive prices remain included in the baseline.

The runtime bound and idleness policy remain provisional because D-28's replay and idleness effects are outside the static source-order model.
The fixed UTC+2 runtime default and paced/idleness verification belong to separate implementation work.
No runtime code or default changes here.

### D-29: Daily analysis interpretations and the Monday checkpoint (2026-09-28, Emilia Repo, issue #4)

Emilia selected these rules in the issue #4 working session: fixed UTC+2 for literal CEST; skip empty windows; carry EMA state over nights and weekends; discard and count events for closed windows; flush at finite-input completion; fill only a missing trading date from system Date; exclude unparseable event timestamps; preserve valid explicit dates.
These choices settle our interpretation without waiting for course-staff clarification.
Existing zero initialization, latest-event-time closes and source-position ties remain unchanged.

The watermark target is zero changed closes against the admit-all reference, with faster bounds measured as ablations.
This does not authorize removing inconvenient events to make a short bound pass.
The full-week summary-row question raised in [the issue discussion](https://github.com/eemelioksanen/scalable-systems-and-data-management/issues/4#issuecomment-5877151366) remains open: under the selected rules those rows are still price events at their supplied time.
D-28's runtime findings qualify the source-order estimates in D-27: actual admission depends on periodic watermark emission, replay pace and idleness.

The initial authorized run was Monday only, followed by human review before another day's download.
After that checkpoint merged in [PR #30](https://github.com/eemelioksanen/scalable-systems-and-data-management/pull/30), Emilia authorized analysis of the remaining six days on 2026-09-29.
It produces independent dataset summaries and close comparisons, using the [daily analysis method](../analysis/README.md).
The full-week oracle already supplies reference totals and anomaly evidence; these are cross-checks, not a substitute for processing the remaining days in this analysis.
See [Monday's results](eda-monday.md).

This issue changes analysis and recorded decisions only.
The parser alignment previously identified as missing has already landed in D-26; no duplicate parser fix is needed.
Flink and oracle default timezone changes remain separate implementation work; existing runtime defaults are still `Z`.

### D-28: Query 2 and subscriptions (2026-09-28, Leo Immonen, issue #8)
Context: Query 2 and D-9/D-17 left open where crossovers are computed, how subscriptions reach the job at startup, when a change takes effect, where counts go and what a rerun guarantees.
Decision:
- **Crossovers:** `EmaCalculator` runs the check when a window fires, from the EMAs before and after it, for every symbol. The result carries the crossover (`EmaResult.crossover`). BUY needs previous EMA₃₈ ≤ EMA₁₀₀ and current EMA₃₈ > EMA₁₀₀; SELL the mirror image. Equal current EMAs produce no advisory.
- **Filter:** a separate `SubscriptionFilter` (keyed by symbol, `subscriptions` as broadcast state) sits after the EMA operator. It keeps each trader's list and, per symbol, how many list entries name it, so a lookup is two map reads whatever the number of traders. Results pass it once; `ema-results` and `advisories` are both written from its output, so they agree at every change.
- **Readiness:** at submission the job reads the topic's end offsets. The filter holds results in keyed state until it has read up to them, then releases them against that state. Malformed records count towards this.
- **Cutover:** after that, a change applies to results the filter processes after it. Nothing is withdrawn or replayed.
- **Finite replay:** `--bounded` also stops `subscriptions` at its end offsets from submission.
- **Counts:** Flink metrics only (listed in interfaces.md). The `stats` topic stays unwritten.
- **Reruns:** no checkpointing, so the supported path is a clean replay into recreated output topics.

Alternatives considered:
- Filtering inside `EmaCalculator` (a keyed broadcast function): saves an operator, but puts every tick through a two-input operator and mixes publication into the calculation.
- No readiness wait: results that beat the subscriptions into the filter are dropped, differently on every run.
- Passing the startup snapshot in the job graph: broadcast state can only be written from the broadcast input, so the snapshot and restored state would have to be merged by hand.
- Per-trader output: rejected by D-17.

Consequences / measurements:
- A trailing transaction marker in `subscriptions` would hold results back forever, so writers must not use transactions. The `subscriptionsReady` gauge shows it.
- Which window a live change first affects depends on timing, since ticks and subscriptions are separate inputs. Correctness runs use a preloaded `["*"]`, which is deterministic.
- A finished bounded job's metrics vanish with it; Prometheus keeps the last 5 s scrape.
- 82 unit and mini-cluster tests pass. They cover the golden fixtures' BUY and SELL, a SELL across empty windows, readiness, overlapping and changed lists, and restoring held results from a snapshot.
- End to end on the local stack, against an independent Python reference of the same rules: six symbols of 08.11.2021, two per exchange, 1,090,752 rows.
  - Bounded, with preloaded subscriptions (overlapping lists, an empty list, a removed trader, a malformed last record): exactly the three subscribed symbols were published, all 311 windows with the reference's closes and EMAs (difference 0), and their 3 BUYs. A second clean run gave identical records apart from `emitTs`.
  - Live, one `ticks` partition, changed at 13:00: `ORA.FR` lost its only subscriber and stopped after 12:55. `RAND.NL` was subscribed and started at 13:00, with EMAs equal to the full-history reference. `lateTicks` = 0, and `evaluatedWindows` 621 = 311 published + 310 suppressed.
- One real day gives each symbol only its warm-up BUY (D-7), so the week is needed for real SELLs. Full week against the oracle (D-27, run on all seven files): ten symbols across FR, NL and ETR, equities and indices, 1,823,412 rows.
  - With `["*"]`: all 5,932 windows and 96 advisories (51 BUY, 45 SELL) equal the oracle's, EMAs bit for bit.
  - Under a subscription schedule: exactly the subscribed symbols were published, equal to the oracle. A second clean run was identical apart from `emitTs`.
  - Live, one `ticks` partition, changed between Wednesday and Thursday: `GET.FR` stopped after Wednesday. `UBI.FR` and `A1EXV6.ETR` started on Thursday, with EMAs and advisories equal to the oracle's full-week values.
- The matching oracle run is the one admitting every event, not the 60 s-bound one. The job counted 0 late ticks: it admitted the 8 summary rows of 10-11 in these symbols (`Trading time` 01:00, sent about 18 h late) that the oracle's 60 s model rejects. In a preload the job's watermark trails far behind what it reads, so whether those rows are late depends on replay speed, not only on the bound. This qualifies D-27, which expects a streaming job to reject them at any bound under about 17.8 h: that holds when the watermark keeps up with the input, as in a live replay, not in a preload. The correctness gate (#9) has to choose its reference from the job's own late count, as the oracle's README describes.
- Finding for D-5: the same live run on 8 partitions lost 209 ticks as late, although the data's own maximum lateness is 283 ms. The pause before the afternoon ticks outlasted the 30 s idleness, so every partition went idle. When ticks resumed, the first partition to catch up advanced the watermark on its own. Live replays need no pauses longer than the idleness timeout.

### D-27: Oracle implementation (2026-09-28, Leo Immonen, issue #5)
Context: D-18 needs a reference that shares no code with the Flink job.
Decision: `oracle/` is a Polars batch job built only from the data contract, this log and `interfaces.md`. Details in [`oracle/README.md`](../oracle/README.md).
- **Source position** (D-23) is (file date from the `DD-MM-YY` in the file name, physical line number). Comment and blank lines keep their line numbers.
- **Open policies are flags:** `--zone-offset` (D-3, as the job's flag) and `--missing-trading-date system-date|drop` (D-10, D-25). Settling them in #4 means a re-run.
- **Arithmetic:** EMAs in binary64 with the spec's formula as written. Every run also recomputes them in 50-digit decimal and reports the drift.
- **Late events:** the oracle admits every event. For each event it records the smallest watermark bound that admits it when events arrive one at a time in source order. The job's watermark never runs ahead of this model except through idleness, so a bound the model says rejects nothing also rejects nothing in the job. To match a run that did reject events, `--exclude-events` takes the job's late events by source file and line (needs #6).
- **Outputs:** the D-18 fields, followed by diagnostic columns (e.g. the source file and line of each close).

Consequences / measurements, on the #4 sample, the 08-11-21 file and the full week (`oracle/evidence/`):
- The sample counts match the EDA profile exactly: 2,389,887 price events, 17,593 with `Last` = 0 and 2,201,703 without `Trading date`. The week has 289,116,808 data rows and 59,391,670 price events (49.1 M index, 10.3 M equity), 4,970 symbols and about 1.86 M windows. The weekend files contain no price events.
- **Monday's 4,840 advisories are all the zero-start BUY of D-7.** From zero, EMA38 stays above EMA100 unless the price falls by about the gap between them, which is still 11% after one day of windows. Tuesday has 176 advisories; Wednesday to Friday have 1,592 to 2,454 each.
- **A burst of summary rows at the end of 10-11:** 5,506 rows at system time 18:56 carry `Trading time` 01:00:00.000. They cover 1,812 FR and NL indices, and their `Last` copies each index's open, high or low. Under D-2 and D-3 they are price events in a 01:05 window. The oracle admits them by default, but a streaming job rejects them as late at any bound under about 17.8 h. The two readings differ in 684,661 windows and 3.7% of advisories. So the correctness check (#9) must compare the job against an oracle run with the job's `--watermark-bound-ms`. (D-30 has since made admit-all the correctness reference, with bounded runs as ablations.) Whether these rows are price events at all belongs to the timestamp policy (D-10, #4).
- **Evidence for D-5, not a decision:** apart from that burst, 352 events in the week arrive after the maximum event time has passed their window's end, and 264 ms admits all of them.
- **Precision against D-18:** prices reach 1.1e6. There, binary64 rounding alone puts EMAs up to 2.3e-9 (33 ulps) from exact, and 1,270 windows in 6 symbols exceed the 1e-9 tolerance. An implementation that orders the same arithmetic differently can therefore fail the check on these symbols without being wrong. D-18 is unchanged; the comparer (#20) should report mismatch magnitudes, and a relative tolerance may be needed.
- The advisories do not depend on arithmetic precision: decimal and binary64 agree on every decision, and no window has `|EMA38 − EMA100|` ≤ 1e-9.

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
- D-25 is still only proposed. The oracle (#5) must apply the same rule, or the two will disagree on nearly every row. (D-29 has since accepted D-25, and the oracle applies it, D-27.)

### D-25: `Date` substitutes for a missing `Trading date` (2026-09-28)
Context: `Trading date` is absent on 92.1% of price rows in the profiled sample.
Decision: build the date part of event time from `Trading date` when present, otherwise from the system `Date`. The time part is always `Trading time`, which is never missing on a price row.
Alternatives considered: dropping rows without a `Trading date` would discard 92% of all trades. Using `Date` unconditionally was rejected only for lack of evidence, not merit — the two never disagree where both are present.
Consequences: **accepted by Emilia on 2026-09-28**, as part of D-29. It is the missing-date rule in D-10. The oracle and the Flink job must apply it identically or they diverge on nearly every row.

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
