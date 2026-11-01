# Live latency validation (#37)

A single real Kafka/Flink run at 100x verifies the measurement path at `065eaa6502ca571ad2149e8541bcea2df9d88f58`.
This is functional evidence on a 12-symbol Monday, not a throughput limit or a repeatability study.

## Reproduce

Use the public DEBS Monday file filtered to the 12 symbols by the command in [bench/README.md](../../../bench/README.md#running-it).
The filtered file has 4,216,894 lines and MD5 `ad78b3955bb96046fe6773f389c66361`.
The validation reused a hardlink to the prepared file and never copied the full dataset.

The environment was macOS arm64, Podman 6.1.4, Compose 5.6.1, and a 7-CPU, 8-GiB local VM.
The host load averages before the run were 1.72, 1.78 and 2.43 on 14 logical CPUs.
Only this benchmark had running containers in the VM.

`bash docs/validation/2026-11-01-latency/reproduce.sh` builds the Java jars with the retained Maven volume, creates task images, and runs [experiment.json](experiment.json).
It requires the cached `trading-trends/kafka:dev` and `trading-trends/viz:dev` images, the prepared input at `data/subset12/`, and free ports 18081, 19090, 39092, 18088 and 13000.
It resets only Compose project `issue37-latency`; do not run it while that project is in use.
Kafka and the producer use 512-MiB heaps so the stack fits the VM.
The producer image uses the verified jar and adds only its heap limit; the Flink image uses the verified jar on `flink:2.2.1-java17`.
The task's Compose override replaces cAdvisor's Docker-specific volumes with read-only `/sys` and disables its Docker-only filter.
Initial setup attempts failed before replay on the Docker mount; the successful replay starts with the narrower override.
The task-local Docker shim also maps correctness containers to `--user 0:0` inside rootless Podman because the macOS group ID is outside its mapping.
The original export failed at container creation on this group ID after the job finished.
Collection was resumed from the same completed run, preserving its original replay, drain times, metrics and job record.
`config.json.collectionRecovery` preserves the failure and the resumed collection times; no completeness rule was relaxed.

After the run, with Python and [bench requirements](../../../bench/requirements.txt) installed:

```bash
python bench/summarize.py bench/results/raw/issue37-latency-subset12-live-100x-<stamp>
python docs/validation/2026-11-01-latency/inspect_run.py bench/results/raw/issue37-latency-subset12-live-100x-<stamp>/r1
```

`inspect_run.py` checks the actual exports, reconciliation, latency completeness and populated latency columns in `runs.csv`, then reports signed model residuals and interval exclusions.
Raw Parquet and full command logs stay in the ignored run directory; the retained checksums identify them.

Clean up after inspection with the same `COMPOSE_PROJECT_NAME`, `COMPOSE_FILE` and port variables used by `reproduce.sh`, using `podman compose down --volumes --remove-orphans`.
Leave unrelated containers, networks, images and the Maven cache alone.


## Results

Run `issue37-latency-subset12-live-100x-20261101T210052Z-r1` replayed 4,216,881 data rows in 461.828 s at exactly 100.0x, with zero send failures and pacing lag p50/p99/max of 0/4/42 ms.
The 25 s warm-up leaves a 436.828 s measurement interval.
All five reconciliation checks pass: 148,465 prices left the source, reached q1, and were exported from `ticks`; 1,388 EMA results and 12 advisories were exported completely; the complete late-event export proves zero late events.
`latency.complete` is true and `runs.csv` contains every stage's p50/p95/p99.

| Stage, milliseconds | p50 | p95 | p99 |
|---|---:|---:|---:|
| Admission to emission | 439 | 3,584 | 30,489 |
| Admission to sink send | 501 | 3,630 | 30,491 |
| Remaining window wait | 101 | 300 | 2,380 |
| Bound wait | 19 | 149 | 153 |
| Partition alignment wait | 140 | 3,001 | 29,249 |
| Queueing, including model error | 160 | 292 | 586 |
| Sink handoff | 70 | 101 | 102 |
| Watermark lag, wall time | 287 | 18,106 | 30,110 |
| Watermark lag, event time | 26,367 | 3,295,093 | 5,249,120 |
| Model ahead, event time | 14,629 | 37,928 | 5,248,119 |

Percentiles do not add; the stage means add to the 1,560.246 ms mean admission-to-output latency.
These are per-result distributions from one run, not ranges across repeated runs.

Of 1,388 results, 1,269 are measured, 117 precede the interval, none follow it, and two are excluded because drain fired them.
The warm-up excludes eight windows of each of 11 symbols and 29 of XEDRI.FR, whose early index snapshot arrived unpaced.
The drain excludes the final 22:30 local windows of WLENV.NL and WLESG.FR, admitted in the last 151 ms of replay and emitted 856 ms after drain began.
There are zero unmodelled results and zero negative queueing residuals; measured residuals range from 27 to 586 ms.
The two drain exclusions are not concealed inside the latency tail.

## Explain the tail and model check

Partitions 3 and 5 contain no prices; the other six are unequal and sparse.
Alignment wait reaches 30,000 ms, matching the idleness timeout, and its p99 is 29,249 ms.
The large latency tail is therefore consistent with quiet partitions holding the watermark, while the measured residual behind model readiness stays below 587 ms.
Fourteen live resource snapshots show zero Kafka pending-record lag and zero backpressure; the broker request-handler idle ratio ranges from 0.9801 to 0.9991.
Sampled container memory totals range from 3.13 to 3.47 GB, below the 8-GiB VM limit.
Podman's CPU percentages, averaged since each container started, sum to 0.62 to 1.06 cores across these snapshots on seven VM cores.
This supports pacing and watermark waits as the limits of this workload; it does not establish maximum sustainable throughput.
cAdvisor scrapes succeeded, but Docker-style per-container resource series are absent under this rootless setup; the independent Podman snapshots are retained instead.

There are 176 operator-watermark samples at 88 distinct scrape times, two q1 subtasks on one TaskManager.
The actual metric name and `operator_name="q1_ema"` label match the harness selector, so no selector fix was necessary.
`modelAheadEventMs` is not near zero in raw event-time units.
At 100x, 200 ms of periodic watermark delay can correspond to 20,000 ms of event time, and sparse-feed jumps make that conversion nonuniform.
The largest model-ahead value, 5,248,119 event ms, occurs at `21:07:02.007Z` on both subtasks.
The model advanced only 110 ms before that scrape; by the next scrape at `21:07:07.003Z`, the observed watermark equals that model value.
This is consistent with sampling across a sparse event-time jump before periodic forwarding, not evidence of 87 minutes of processing delay.
The raw outlier remains in the percentiles and in `diagnostics.json.largeModelJumps`.
The 5 s scrape cadence cannot establish the exact crossing time.

## Collection cost and checks

The real run exposed a missing decoder: the live producer compresses `ticks` with LZ4, and export failed with `UnsupportedCodecError`.
`correctness/requirements.txt` now enables the installed Kafka client's declared LZ4 extra at the same client version.
Successful collection used kafka-python 3.0.11 with lz4 4.4.5, now pinned for reproduction.
Collection was resumed from the retained completed run rather than repeating replay or changing output timestamps.

A separate read of the completed topics, using the exporter's exact decoder, took 20.473 s for all 4,216,881 ticks and produced 148,465 price rows.
Reading the 1,400 output records and the empty late topic took 0.149 s.
The added ticks reread alone is 4.43% of the 461.828 s replay duration; it runs after the measured interval and is not part of reported result latency.
This is one warm reread, excludes consumer startup, Parquet sorting/writing and readiness modelling, and is not the full collection overhead.
`collection_cost.py` and `collection-cost.json` retain the procedure and all offsets.
Run it inside the correctness image on `issue37-latency_default` with the script mounted read-only and the correctness package on `PYTHONPATH`.

Maven `clean verify` passed 37 producer, 93 Flink and 38 viz tests.
The correctness suite passed all 68 tests both on the host and in the rebuilt exporter image.
The container test invocation mounts `correctness`, `oracle`, `fixtures` and `bench` at their root-level names; the benchmark-importing test needs `/bench/run.py`.
The evidence checker and `bench/summarize.py` completed successfully.
The host/JobManager clock probe measured +38 ms with a 149 ms round trip; its midpoint estimate is uncertain by at least half that round trip and does not support sub-millisecond cross-host claims.

## Measurement limits

Admission is the input record's `CreateTime`, set before producer send; it is not broker append.
Output `CreateTime` is the sink producer's send time; broker publication and consumer visibility remain unobserved.
Readiness is a model driven by admitted prices, not a measured per-window watermark crossing.
Its error and periodic watermark forwarding are included in signed `queueing`.
All service timestamps share the local VM's kernel clock; the harness also records a host-to-JobManager clock probe and its round-trip uncertainty.
No multi-VM synchronization was exercised.
No historical event timestamp is subtracted from wall-clock time.

The run does not include an oracle comparison, so harness `COMPLETE` means counts and export reconciliation here, not an independent verdict on EMA values.
Local checks passed all 168 Java tests and 68 correctness-tool tests; this pass changes no job logic.
No repeat-to-repeat range or performance improvement is claimed from one run.

## Cleanup

All `issue37-latency` containers and its network were removed after collection and checks.
The eight pre-existing stopped `trading-trends` containers, their network, unrelated volumes, and the Maven cache were left in place.
No cloud resources, other branches, human comments, merge, or PR watch were used.
