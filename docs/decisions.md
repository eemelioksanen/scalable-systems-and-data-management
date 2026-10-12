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
| D-5 | Window firing | Discard and count events for closed windows; flush remaining windows at finite-input completion. Evaluate bounded out-of-orderness and idleness settings separately. | **late/EOF policy accepted; bound and idleness open** | Emilia selected the late/EOF policy on 2026-09-28. The bound targets zero changed closes; faster bounds are ablations. Monday alone cannot settle the full-week bound, and D-28 shows replay timing and idleness matter. See D-29. |
| D-6 | Empty windows | No price event in a window: EMA unchanged, no output, no crossover check. | **accepted** | Emilia selected skip on 2026-09-28. The carry-forward alternative is an ablation. |
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
| D-13 | `ticks` content | The replayer forwards every CSV row as 7 raw columns (`ID, SecType, Date, Time, Last, Trading time, Trading date`). All parsing and normalization happen in Flink. | accepted | Normalization belongs to the system and is described in the report. Early filtering in the replayer is a later design comparison. |
| D-14 | `ticks` encoding | UTF-8 CSV string, verbatim fields, empty = null, no quoting. | accepted | Compare against a binary encoding later as a design comparison. |
| D-15 | `ticks` key and partitions | Key = symbol, default partitioner, 8 partitions locally. The record timestamp is the replayer's send time. | accepted | Revisit the partition count for the cloud runs. The Kafka offset gives per-symbol arrival order for D-4 ties. |
| D-16 | Output records | JSON. `ema-results`: `symbol, secType, windowEnd, close, ema38, ema100, maxIngestTs, emitTs`. `advisories`: `symbol, secType, windowEnd, type (BUY/SELL), ema38, ema100, maxIngestTs, emitTs`. | accepted | Which latency definition to report is still open. `secType` enables equity/index filtering in the viz. |
| D-17 | `subscriptions` | Key = trader ID. Value = JSON symbol list (`["*"]` = all). A tombstone removes the trader. Compacted topic. | accepted | Results are emitted once per symbol. Fan-out to traders happens in the viz backend. |
| D-18 | Correctness check | The oracle writes Parquet with the same field names. EMAs are joined on (symbol, windowEnd) with 1e-9 absolute tolerance and exact `close`. Advisories are compared as sets of (symbol, windowEnd, type), reporting precision and recall. | accepted | Flink runs with a `["*"]` subscription for the check. |

## Log

Add later decisions below, newest first.

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

Consequences / measurements (2026-10-12, rootless Podman VM with 7 CPUs and 8 GiB, 8 partitions, one TaskManager with 2 slots; 12 symbols of 08 to 10 November, 13,810,579 rows, admit-all oracle):
- Bounded preload at parallelism 2: reconciliation exact (13,810,579 read, 492,434 price events, 4,166 windows, 17 advisories), 0 EMA and 0 advisory differences, precision and recall 1.0. The gate fails only because `lateTicks` was never scraped.
- Drained preload: the source's counters matched the oracle's input split exactly (492,434 price, 13,318,109 non-price, 36 non-positive, 182,540 dated from `Date`), `lateTicks` was final at 0, and the gate passed.
- Live replay of 1.5 M rows of 08 November at 100× from 09:00, with the provisional 60 s bound and 30 s idleness: pacing achieved 100.0×, p99 send lag 7 ms; all counts reconciled and `lateTicks` was 0.
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
- The replay clock shows two simulated times next to wall-clock time: the feed time from the tail of `ticks`, read with the replayer's source-clock rule, and the latest evaluated window. The gap between them shows the watermark delay.
- The heatmap shows every published symbol, sorted by its latest sign flip of EMA38 − EMA100. A symbol's first window counts as a flip, matching the BUY that zero initialization gives every symbol (D-7).
- Prices carry no currency label: no topic carries the currency (D-24, D-16), and the chart says so.

Consequences:
- After a preload the feed time shows the end of the input at once. It is only meaningful during a live replay.
- #13 adds `advisories` as another consumer feeding the same store, and an extra field in the WebSocket messages.

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
