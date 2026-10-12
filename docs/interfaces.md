# Interfaces

> Measured evidence for these formats, and the corrections D-21 to D-25 make to
> them, are in [data-contract.md](data-contract.md).

These are the contracts between the replayer (`producer/`), the Flink job (`flink-job/`), the oracle (`oracle/`) and the visualization (`viz/`). Change them only through a new entry in [decisions.md](decisions.md) (D-13 to D-18 cover this document), so nobody codes against a stale format.

## Kafka topics

| Topic | Written by | Read by | Key | Value | Partitions |
|---|---|---|---|---|---|
| `ticks` | replayer | Flink job; viz reads the tail for its replay clock | symbol (`ID`) | CSV line, 7 raw fields | 8 locally (`TICKS_PARTITIONS`) |
| `ema-results` | Flink job | viz, correctness check | symbol | JSON | 8 |
| `advisories` | Flink job | viz, correctness check | symbol | JSON | 8 |
| `subscriptions` | subscription generator | Flink job (broadcast) | trader ID | JSON, or a tombstone | 1, compacted |
| `stats` | not written yet: counts are Flink metrics (D-28, [Counts](#counts-d-28)) | – | – | – | 1 |

All keys and string values are UTF-8. Partition counts are revisited for the cloud runs.

## `ticks` (D-13, D-14, D-15, D-31)

The replayer sends raw data and does no parsing. All parsing, timestamp normalization and event classification happen in the Flink job. How to run it, and the measurements behind its choices, are in [replayer.md](replayer.md).

- **One record per CSV data row**, including non-price and housekeeping rows. Comment lines (`#…`) and any header row are skipped. A row that is neither 39 fields nor 40 with an empty last one cannot be projected: it is counted as `rejected` in the replay report and not sent. The profiled weekday has none.
- **Key:** the `ID` column, as-is. The default Kafka partitioner is used.
- **Value:** a projection of 7 of the CSV's 39 columns, in this order (D-24). Fields are comma-separated, an empty field means null, and there is no quoting. Note the source rows carry 40 fields, the last one empty (D-21):

  ```
  ID,SecType,Date,Time,Last,Trading time,Trading date
  ```

  Illustrative examples, first a price event and then a quote or housekeeping event:

  ```
  RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.100,08-11-2021
  RDSA.NL,E,08-11-2021,09:00:01.456,,,
  ```

- **Header `src`:** the record's source position, `<file index>:<line number>` in ASCII, e.g. `0:14`. The file index points into the replay report's `files`, and lines count from 1 including comment lines. It is the oracle's `(sourceFile, sourceLine)`, with `sourceFile` the file name of `files[index].path`, which lets late events be matched to the oracle's `--exclude-events`. The Flink job does not read it yet: it counts its late events but does not export them. Consumers of the value can ignore it.
- **Record timestamp:** the wall-clock time (epoch ms) when the replayer sent the record. This is the ingest time used for latency measurements.
- **Compression:** lz4 by default (D-31). Consumers decompress transparently.
- **Ordering:** the replayer keeps file order per symbol: day files in date order, lines in file order. It sends from one thread with an idempotent producer and `acks=all`, so retries don't reorder records.
  - Because the key is the symbol, all of a symbol's events land in one partition. **The Kafka offset is therefore the arrival order** used to break ties when picking a window's close (D-4).
  - The oracle uses (file date, line number) as the same arrival order.
- **Completion:** a finite replay ends with a JSON report (D-31). Only a report with `"status": "COMPLETE"` describes a whole input. Its `offsets` give each partition's `[startOffset, endOffset)`, which hold exactly this run's records when `endOffset - startOffset = records`. A `FAILED` replay leaves a partial input in the topic, so reset `ticks` before retrying.

## `ema-results` (D-16)

The Flink job emits one JSON record per (symbol, window) when **both** of these hold:

- the window had at least one price event (D-6), and
- at least one trader subscribes to the symbol (D-9).

```json
{"symbol":"RDSA.NL","secType":"E","windowEnd":1636355100000,"close":19.28,"ema38":19.2712,"ema100":19.2655,"maxIngestTs":1790000000123,"emitTs":1790000000456}
```

| Field | Type | Meaning |
|---|---|---|
| `symbol` | string | Symbol key (D-1). |
| `secType` | string | `E` (equity) or `I` (index), copied from the input event. The viz uses it for filtering. |
| `windowEnd` | long | Exclusive end of the 5-minute window, epoch ms under the timezone interpretation of D-3 (`--zone-offset`, default `+02:00`): the window ending 09:05 on 08-11-2021 is `1636355100000`. |
| `close` | double | Price of the window's closing event (D-4). |
| `ema38`, `ema100` | double | EMA values after this window, written at full precision (no rounding). |
| `maxIngestTs` | long | Latest record timestamp among the window's price events. |
| `emitTs` | long | Wall-clock time (epoch ms) when the Flink job emitted the record. |

The last two fields support the latency definitions still under consideration. Which one is reported is an open decision.

## `advisories` (D-16)

The Flink job emits one JSON record per detected crossover of a subscribed symbol:

- `BUY` when equations (1) and (2) of the spec hold (EMA₃₈ crosses above EMA₁₀₀): previous EMA₃₈ ≤ EMA₁₀₀ and current EMA₃₈ > EMA₁₀₀.
- `SELL` when equations (3) and (4) hold (EMA₃₈ crosses below): previous EMA₃₈ ≥ EMA₁₀₀ and current EMA₃₈ < EMA₁₀₀.

"Previous" is the symbol's last window with a price event, since empty windows are skipped (D-6). Before the first window both EMAs are 0 (D-7), so every symbol's first window is a `BUY`. Equal current EMAs produce no advisory.

Crossovers are computed for every symbol, subscribed or not, so a symbol's first advisory after a subscription is still measured against its unpublished history. Every advisory has an `ema-results` record for the same (`symbol`, `windowEnd`) with the same EMAs: the two are filtered together.

```json
{"symbol":"RDSA.NL","secType":"E","windowEnd":1636355100000,"type":"BUY","ema38":19.2712,"ema100":19.2655,"maxIngestTs":1790000000123,"emitTs":1790000000456}
```

The fields mean the same as in `ema-results`.

## `subscriptions` (D-17)

- **Key:** the trader ID, e.g. `trader-0042`.
- **Value:** `{"trader":"trader-0042","symbols":["RDSA.NL"]}`. This replaces that trader's previous list.
  - `trader` may be omitted. If present, it must equal the key.
  - `"symbols":["*"]` subscribes to every symbol. Correctness runs use this.
  - `"symbols":[]` subscribes to nothing. Listing a symbol twice is harmless.
  - A **tombstone** (null value) removes the trader.
  - Any other value, or a record without a key, is counted as `malformedSubscriptions` and ignored.
- A symbol is published while at least one trader lists it (or `*`).
- Compaction keeps the latest list per trader. A (re)started job reads the topic from the beginning to rebuild the current subscription state.
- Writers must not use Kafka transactions: the job treats the record at a partition's end offset − 1 as the last one to wait for (below), and a transaction marker there would hold results back forever.
- The Flink job emits each result once per symbol, not once per trader. Delivering results to individual traders is the visualization backend's job.

**Startup (D-28).** At submission the job reads the topic's end offsets. Until it has read the topic up to them, it holds every result back; then it publishes the held results that the subscriptions allow. So everything is judged against at least the subscriptions that existed at submission, whichever input the job happens to read first. To get deterministic output, write the subscriptions **before** submitting the job. The `subscriptionsReady` gauge is 1 once this has happened.

**Changes while running.** A change applies to every result the subscription filter processes after it. Results already published are not withdrawn, and a new subscription does not replay earlier windows: its first result is the symbol's next evaluated window. Because ticks and subscriptions are separate inputs, which window that is depends on timing.

**Finite replay.** With `--bounded`, the job reads `subscriptions` only up to the end offsets at submission, like `ticks`. Later changes are ignored.

## Counts (D-28)

The job's counts are Flink metrics, exported to Prometheus (see the README). Nothing writes the `stats` topic yet.

| Operator | Metrics | Meaning |
|---|---|---|
| `ticks` source | `priceEvents`, `nonPriceEvents`, `nonPositivePriceEvents`, `missingTimestampEvents`, `malformedEvents` | Split every `ticks` record: each counts in exactly one. |
| | `systemDateEvents` | Price events dated from `Date` (D-25), a subset of `priceEvents`. |
| `q1-ema` | `lateTicks` | Price events whose window had already been evaluated. |
| | `evaluatedWindows`, `buyCrossovers`, `sellCrossovers` | Windows and crossovers of every symbol, subscribed or not. |
| | `emaStates`, `openWindows` (gauges) | Per subtask: symbols whose EMAs it holds, and windows holding a close candidate. They count state entries, not bytes (D-35). |
| `subscriptions` source | `subscriptionUpdates`, `subscriptionTombstones`, `malformedSubscriptions` | Split every `subscriptions` record. |
| `subscription-filter` | `publishedResults`, `publishedAdvisories`, `suppressedResults` | At the end of a run, `publishedResults` + `suppressedResults` = `evaluatedWindows`. |
| | `subscriptionsReady` (gauge) | 1 once the startup subscriptions have been read. |

A finished `--bounded` job's metrics disappear with it. Prometheus keeps what it last scraped, every 5 s, so read the final counts from the Flink UI or REST API before the job ends if they have to be exact.

## Metrics (D-35)

Prometheus scrapes three jobs every 5 s:

| Job | Exporter | Covers |
|---|---|---|
| `flink` | Flink's Prometheus reporter, port 9249 on the JobManager and every TaskManager | Throughput, lag, busy and backpressured time, records and state per subtask, Flink's JVMs |
| `kafka` | Prometheus JMX exporter agent 1.7.0 in the broker, port 9404, rules in [`infra/kafka/jmx-exporter.yml`](../infra/kafka/jmx-exporter.yml) | Messages and bytes per topic, end offset per partition, broker idle ratios, the broker's JVM |
| `cadvisor` | cAdvisor 0.60.6 | CPU, memory and network per container and for the Docker host |

Each measurement and its query:

| Measurement | Query | Job |
|---|---|---|
| Raw ticks into Kafka, records/s | `sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="ticks"}[15s]))` | `kafka` |
| Raw ticks read by the job, records/s per subtask | `sum by (subtask_index) (rate(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{operator_name="Source:_ticks"}[15s]))` | `flink` |
| Price events, events/s | `sum(rate(flink_taskmanager_job_task_operator_deserializer_ticks_priceEvents[15s]))` | `flink` |
| Lag, records | `sum(flink_taskmanager_job_task_operator_pendingRecords{operator_name="Source:_ticks"})` | `flink` |
| Lag from the broker's end offsets | `sum(kafka_log_logendoffset{topic="ticks"}) - sum(flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset{topic="ticks"} + 1)` | `kafka`, `flink` |
| Idle and backpressured time, ms/s per subtask | `rate(flink_taskmanager_job_task_accumulateIdleTimeMs[15s])`, `rate(flink_taskmanager_job_task_accumulateBackPressuredTimeMs[15s])`, by `task_name` and `subtask_index`; busy is 1000 minus both | `flink` |
| Records per subtask | `flink_taskmanager_job_task_operator_numRecordsIn{operator_name="q1_ema"}`, by `subtask_index` | `flink` |
| State entries per subtask | `flink_taskmanager_job_task_operator_emaStates`, `flink_taskmanager_job_task_operator_openWindows` | `flink` |
| State bytes | `flink_jobmanager_job_lastCheckpointFullSize`, only when the job is submitted with checkpointing; per subtask from the REST API's checkpoint details | `flink` |
| CPU, cores | `max by (name) (rate(container_cpu_usage_seconds_total{name!=""}[15s]))`; the host is `id="/"` | `cadvisor` |
| Memory, bytes | `max by (name) (container_memory_working_set_bytes{name!=""})`; the host is `id="/"` | `cadvisor` |
| Peak memory, bytes | `max by (name) (max_over_time(container_memory_working_set_bytes{name!=""}[10m]))` over the run; `container_memory_max_usage_bytes` is the cgroup's peak since the container started, page cache included | `cadvisor` |
| JVM heap, bytes | `flink_taskmanager_Status_JVM_Memory_Heap_Used`, `jvm_memory_used_bytes{job="kafka", area="heap"}` | `flink`, `kafka` |
| GC time | `flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond` (ms/s), `rate(jvm_gc_collection_seconds_sum{job="kafka"}[30s])` (s/s) | `flink`, `kafka` |
| Network per container, bytes/s | `sum by (name) (max by (name, interface) (rate(container_network_receive_bytes_total{name!=""}[15s])))`, and `transmit` | `cadvisor` |
| Network per topic, bytes/s | `sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total[15s]))`, and `bytesout` | `kafka` |
| Broker saturation, 1 = idle | `kafka_server_request_handler_avg_idle_ratio` (a one-minute average), `kafka_network_processor_avg_idle_ratio` | `kafka` |

- Flink's `currentOffset` is the last record read, -1 before the first, hence the `+ 1`.
- The job commits no offsets without checkpointing (D-28), so Kafka has no consumer-group lag for it.
- Podman reports a container under several cgroups. The largest is the container itself, which `max by (name)` picks; Docker should report one series per container, but no Docker run has checked that. Network series are per interface, so the network query takes the maximum per interface before summing; cAdvisor leaves out loopback.
- A `[15s]` rate spans three scrapes, so it smooths bursts shorter than that.
- Flink's own `busyTimeMsPerSecond`, `idleTimeMsPerSecond` and `backPressuredTimeMsPerSecond` are 60 s averages, and busy reads 1000 until a task's first 5 s update. The rates of the accumulated idle and backpressured times follow a run within about 5 s. Busy is taken as the rest: Flink computes `accumulateBusyTimeMs` as wall time minus an idle time that only updates every 5 s, so its rate can exceed 1000 ms/s.
- To get state bytes, submit with `-Dexecution.checkpointing.interval=10s -Drestart-strategy.type=none`. Without the restart setting, checkpointing would also restart a failed job instead of failing it (D-28). The state gauges count from the job's start, so they are wrong after any restore from a checkpoint or savepoint.

## Restart and replay (D-28)

Checkpointing is not enabled, so the job does not recover from a failure: it fails. The supported way to run again is a **clean replay**:

1. Recreate `ema-results` and `advisories`, or write to new topics with `--ema-topic` and `--advisories-topic`.
2. Submit the job again.

Both inputs are read from the earliest offset, so the same `ticks` and a preloaded `subscriptions` give the same records apart from `emitTs`. Without step 1 the outputs hold a second copy of every record. The sinks are at-least-once, so a checkpoint restore would also be able to duplicate records if checkpointing were turned on later.

## Oracle output and correctness check (D-18)

- **Oracle output:** `oracle/out/ema_results.parquet` and `oracle/out/advisories.parquet`. Both are gitignored.
  - They use the same field names as the topics, without `maxIngestTs` and `emitTs`.
  - Diagnostic columns follow those fields (e.g. the source file and line of each close); [`oracle/README.md`](../oracle/README.md) lists them. Select the D-18 fields by name.
  - They cover all symbols.
- **Flink side:** a dump of `ema-results` and `advisories` from a run with a `["*"]` subscription.
- **EMA check:** join on (`symbol`, `windowEnd`). A row is a mismatch if any of these holds:
  - `close` differs at all
  - `ema38` or `ema100` differs by more than **1e-9** (absolute)
  - the row exists on only one side

  Report the mismatch counts. They must all be 0.
- **Advisory check:** compare the two sets of (`symbol`, `windowEnd`, `type`). Report precision and recall. Both must be 1.0.
- **Tools:** [`correctness/`](../correctness/README.md) exports the topics, runs both checks and classifies every difference (D-34).
