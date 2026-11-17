# Benchmark harness

`bench/run.py` runs an experiment on the Compose stack: it resets the stack, writes the subscriptions, replays the input, runs the Flink job to completion, and keeps everything the run measured. `bench/summarize.py` turns retained runs into CSV tables and plots with the mean and standard deviation over repeats. The design is D-36 in [decisions.md](../docs/decisions.md).

Results are tuning data until the full-week correctness gate (#9) passes. Publish no performance conclusion before that, whatever the harness reports.

## Running it

`run.py` needs Python 3.9+ and Docker; it uses only the standard library. `summarize.py` needs a venv:

```bash
python3 -m venv bench/.venv
bench/.venv/bin/pip install -r bench/requirements.txt
```

Build the images, then run an experiment and summarize it:

```bash
python3 bench/run.py bench/configs/subset12-preload-p2.json
bench/.venv/bin/python bench/summarize.py bench/results/raw/subset12-preload-p2-<stamp>
bench/.venv/bin/python bench/summarize.py bench/results/raw/subset12-preload-p1-<stamp> \
  bench/results/raw/subset12-preload-p2-<stamp> --out bench/results/raw/parallelism
```

`run.py` builds the images first unless given `--no-build`, and `--repeats N` overrides the config. It exits nonzero unless every repeat ends `COMPLETE` (see [Status](#status)). It takes over the Compose project `trading-trends`: every repeat runs `docker compose down --volumes`, so stop anything else using that stack first. The stack stays up after the last repeat for inspection.

The example configs use `data/subset12/`, 12 symbols of 08 to 10 November (13,810,579 rows). It is made from the day files, and these commands reproduce its MD5s:

```bash
mkdir -p data/subset12
for d in 08 09 10; do f=debs2022-gc-trading-day-$d-11-21.csv
  LC_ALL=C grep -E '^(#|ID,|(RDSA\.NL|TTE\.FR|ASML\.NL|AIR\.FR|823212\.ETR|766403\.ETR|623100\.ETR|710000\.ETR|716460\.ETR|WLENV\.NL|WLESG\.FR|XEDRI\.FR),)' \
    data/daily/$f > data/subset12/$f
done
oracle/run.sh data/subset12/*.csv --out /out/subset12
```

## Config

| Key | Meaning |
|---|---|
| `name` | Experiment name; the results go to `bench/results/raw/<name>-<UTC stamp>/`. |
| `mode` | `preload`: replay the input, then run the job on it. `live`: start the job, then replay in real time (`--mode realtime`). |
| `finish` | How the job ends. `bounded` (preload default): submitted with `--bounded`, so its duration is exact. `drain`: submitted unbounded; once it has read every record, `flink stop --drain` evaluates the remaining windows. Live runs always drain. |
| `input` | Files or directories under `data/`, passed to the replayer as `/data/...`. |
| `replayArgs` | Further replayer options: `--rate` and `--pace-from` (required `--rate` in live runs), `--exchanges`, `--symbols`, `--limit`. The harness sets `--mode` and `--run-id`. |
| `job` | `watermarkBoundMs`, `idlenessMs` and `zoneOffset`, all required, and optional extra `args`. The harness has no defaults for the bound and idleness, so every record states them. The job's own are 64265001 and 0 with `--bounded`, and 1000 and 30000 live (D-38). |
| `variant` | The design-comparison variant ([Variants](#variants-d-42)). Optional; each missing key is the baseline. |
| `stack` | `partitions` (`TICKS_PARTITIONS`), `taskmanagers` (`--scale`), `slots` (`TM_SLOTS`), `parallelism` (`PARALLELISM`). Defaults 8, 1, 2, 2. |
| `checkpointing` | `true` submits with `-Dexecution.checkpointing.interval=10s -Drestart-strategy.type=none`, which state bytes need (D-35). Default `false`. It needs `"finish": "drain"`: a bounded job would wait for a final checkpoint before it ends, and a few-second one takes none. |
| `subscriptions` | Trader records written to `subscriptions` before the job starts. Default `[{"trader":"all","symbols":["*"]}]`. |
| `repeats`, `warmupS`, `timeoutS` | Repeats; seconds excluded from the start of the measurement interval; how long the replay, the job and the drain may each take (default 3600). Startup waits have fixed limits of a few minutes. |
| `oracle` | An oracle output directory for the correctness check. The harness refuses one computed from other files than `input`, and one combined with `--limit`, `--symbols`, `--symbols-file` or `--exchanges`, since the oracle covers whole files. For a live run, use an oracle run at the job's bound: a live job rejects the burst that the admit-all output includes (D-38). |

## Variants (D-42)

`variant` picks one alternative per key. The first value is the baseline and the default:

| Key | Values | Passed as |
|---|---|---|
| `operator` | `process`, `window` | the job's `--operator` |
| `emptyWindows` | `skip`, `carry-forward` | the job's `--empty-windows` |
| `stateBackend` | `hashmap`, `rocksdb` | the job's `--state-backend` |
| `ticksEncoding` | `csv`, `binary` | the job's `--ticks-encoding` and the replayer's `--encoding`; a live run's export decodes `ticks` with it |
| `filtering` | `flink`, `replayer` | the replayer's `--early-filter` for `replayer` |

The harness always passes all of them, so a baseline run states its variant too. `effective.variant` in `config.json` records the request. After submitting, the harness reads the configuration the job resolved from Flink's `/jobs/<id>/config` into `job.effective`, and fails the run if any of it differs from the request. `runs.csv` has a column per key.

With `carry-forward`, `oracle` must be an oracle run with `--empty-windows carry-forward`; the harness refuses any other. A live carry-forward run reports no latency stages: its carried windows have no admission of their own, so `latency.json` says `complete: false`. With `filtering: replayer`, the job counts no non-price events, so the reconciliation adds the replay report's `filtered` to the job's count before comparing with the oracle's. [The variant check](../docs/validation/2026-11-17-variants/README.md) runs every variant on the semantics fixtures, and its configs are templates for #11.

## What a run does

1. **Reset.** `docker compose down --volumes`, then `up -d --scale taskmanager=N`. It waits for `kafka-init` to exit, Flink to register the TaskManagers and slots, and Prometheus to scrape every Flink, Kafka and cAdvisor target.
2. **Subscriptions** are written before the job is submitted, so the output is deterministic (D-28).
3. **Replay and job.** A preload replays everything with the job stopped, then submits it. A live run submits the job, waits for all its tasks to run, then replays at the configured rate. The replay has to start within the job's idleness timeout. After that every partition is idle, and the unpaced rows before `--pace-from` can be rejected as late (D-38). `config.json` has both times.
4. **Completion.** A bounded job is done when Flink reports it `FINISHED`. A drained job is done once the sum of the job's Kafka offsets reaches the replay's end offsets and q1-ema has received every price event, by Prometheus; the harness waits one more scrape, then stops it with `--drain`. A job that fails or is cancelled meanwhile stops the run at once.
5. **Collection**, before the next reset deletes it: Flink's record of the job, Prometheus range queries, the job's counters, the effective configuration, and an export of `ema-results` and `advisories` with `correctness/run.sh export`. With an `oracle`, `correctness/run.sh compare` runs too.

The **measurement interval** is the job's run in a preload and the replay in a live run, both minus `warmupS` at the start. Repeated runs use 15 s for preloads and 25 s for live runs (D-37). It bounds the metric summaries in `summary.json`. Preload time is never inside it: it is the replayer's own `durationMs` and `recordsPerSecond`, kept separately (D-35: a short preload gives Prometheus two or three scrapes).

**Job throughput** is the records the `ticks` source read divided by the time from its first subtask's start to its last subtask's end, from Flink's record of the job. That leaves out scheduling and deployment, about 9% of a 3.5 s job. Only bounded runs report it: a drained job's tasks run on until the harness stops them.

## Status

`config.json` records each run's `status`:

| Status | Meaning |
|---|---|
| `COMPLETE` | The job finished, the counts reconciled, and the correctness check (if any) passed or is `unverified`. |
| `INVALID` | The job finished, but reconciliation failed or the correctness check found differences. Everything is kept, and `summarize.py` leaves it out of the means. |
| `FAILED` | A step failed, including the job ending `FAILED` or `CANCELED`. `error` says which; Flink's record of the job and the Prometheus history are kept when the job had started. |
| `INTERRUPTED` | Stopped by Ctrl-C. |

## Output

Each repeat writes `bench/results/raw/<name>-<stamp>/r<k>/`. The run ID is `<name>-<stamp>-r<k>`, and the replayer's report carries it too.

| File | Contents |
|---|---|
| `config.json` | The config record: the experiment config and its `effective` values, the code revision and whether the tree was dirty, input files with size and MD5, checkpointing, the job's submitted arguments and ID, host hardware, the container engine's `info`, every container's image ID and CPU and memory limits (0 = none), Flink's TaskManagers with their slots and memory, Flink's configuration, the JobManager's clock offset from the host, and the phase timestamps. Written even when a run fails, with its error. |
| `compose.yml` | `docker compose config` for this run: the stack as Compose resolved it. |
| `replay.json` | The replayer's report: status, counts, offsets per partition, timing, pacing. |
| `flink-job.json` | Flink's REST view of the finished job, per vertex and subtask: records and bytes in and out, busy, idle and backpressured time. Flink keeps it after the job ends, so these counts are exact. |
| `metrics.json` | Every D-35 query as a 5 s range query over the run, raw, and `q1-ema`'s input watermark with the time of each scrape. |
| `export/` | The correctness export: `ema_results.parquet`, `advisories.parquet` (with `maxIngestTs`, `emitTs` and each record's `kafkaTs` and `kafkaTsType`), `late_events.parquet` and its `manifest.json`. A live run adds `price_events.parquet`: each price event's partition, offset, record timestamp and event time. |
| `latency/` | Live runs: `latency.json` ([Latency](#latency)). |
| `compare/` | With an `oracle`: the comparison report and differences ([correctness/README.md](../correctness/README.md)). |
| `summary.json` | What the run derived: throughput, per-subtask load and skew, the metric summaries over the measurement interval, the counters, the reconciliation and the correctness verdict: `pass`, `unverified` (no differences, but `lateTicks` unknown) or `fail`. A live run adds `latency`. |
| `commands.log` | Every command and its stderr. |

`summarize.py` writes into the experiment directory, or `--out`:

| File | Contents |
|---|---|
| `runs.csv` | One row per run: status, stack size, replay and job throughput, subtask read skew (max over mean), busy ratios, output counts, `lateTicks` when final, reconciliation and correctness verdict. A live run adds the latency percentiles of each stage and of the watermark lag, the measured results and those excluded or unmodelled. A busy ratio is 1 − (idle + backpressured) / duration over the tasks' whole run, from Flink's record of the job (D-35's definition of busy). The busy skew is the busiest subtask's busy time over the mean. CPU cores (the TaskManagers' summed, Kafka's, the replayer's and the host's) and the broker's request handler idle ratio are means over the measurement interval. |
| `summary.csv` | Per experiment and metric: n, mean, sample standard deviation, min and max, over the `COMPLETE` runs. |
| `throughput.png` | Job and replayer records per second: mean, standard deviation and each run. |
| `timeseries.png` | Ticks read per second and lag over time, one line per repeat. A preload of a few seconds has no samples. |

## Predicting subtask load

`bench/predict_load.py` predicts the records each `ticks` source subtask reads and the price events each `q1-ema` subtask receives, from the week's per-symbol counts in `analysis/out/week/symbols.csv`. It assigns partitions to source subtasks as the Kafka connector's enumerator does, and symbols to `q1-ema` subtasks as Flink's `keyBy` does. On the full week it matched every measured subtask exactly at parallelism 1, 2, 4, 6 and 8, on 8 and 16 partitions and on exchange subsets ([baselines](../docs/validation/2026-10-12-baselines/README.md)), so a partition count or parallelism can be checked for skew without a run:

```bash
bench/.venv/bin/python bench/predict_load.py --partitions 8 16 --parallelism 4 8 16
bench/.venv/bin/python bench/predict_load.py --partitions 8 --parallelism 4 --exchanges FR
```

## Counts and reconciliation

A bounded job's custom counters vanish when it finishes, and on a short run Prometheus never scrapes them: a 13.8 M-record preload is read in about 3.5 s. Flink's REST record of the job keeps each vertex's record counts, so reconciliation uses those:

- the replay's acknowledged records = records the `ticks` source read
- price events the source emitted = records q1-ema received
- in a live run, price events the source emitted = price events exported from `ticks`
- records the sink chain wrote = records exported from both topics
- with a `*` subscription, windows q1-ema evaluated = exported `ema-results`
- with an oracle, replayed data rows and price events equal the oracle's, and, when the source's counters are final, so do the non-price, non-positive and `Date`-dated events

The source counters come from Prometheus's last sample. `countersFinal.split` is true when they reconcile with the records the source read. `lateTicks` comes from the completed `late-ticks` export, with source filenames resolved through this run's `replay.json`; `countersFinal.lateTicks` records export completeness. This works for bounded and drained jobs. `compare` receives both the count and exported source identities. The q1 output reconciliation includes both evaluated windows and late events. Window, crossover and publication counters remain sampled; the job graph's counts replace them in reconciliation.

## Latency

A live run exports `ticks`' price events with the outputs, and `correctness/run.sh latency` writes `latency/latency.json`, which `summary.json` carries as `latency` (D-40). It refuses an export whose `ticks` or `ema-results` part is incomplete and reports `complete: false` instead, and reconciliation requires the exported price events to equal those the source emitted. Preloads report no latency: their records enter Kafka before the job starts.

### What each timestamp is

All of them come from one clock. The replayer, the TaskManagers and Prometheus run as containers on one host and read its kernel clock.

| Timestamp | Where it comes from | What it is not |
|---|---|---|
| admission, `maxIngestTs` | the `ticks` record timestamp of the window's last price event: `CreateTime`, set by the replayer when it hands the row to its Kafka producer | the broker's append, which comes later and is not observed |
| emission, `emitTs` | the TaskManager's clock when `q1-ema`'s timer fires and evaluates the window | |
| output, `kafkaTs` | the `ema-results` record timestamp, kept with its `kafkaTsType`. Under `CreateTime`, the topics' default, the sink's Kafka producer stamps it when `send()` is called | publication: the broker's append and a consumer seeing it are not observed. Only a `LogAppendTime` topic would make it the append time |
| readiness | modelled: when the watermark would pass the window's end if every price event reached the job the instant it was admitted | an observation: Flink's watermark gauges are sampled every 5 s |

No 2021 event time is ever subtracted from a wall-clock time. Event times only decide when the feed and the watermark reached a window's end.

### Stages

For each result, the stages run along one chain of instants. Each instant is at least the one before it, except emission, so the stages add up to the total.

| Stage | From | To |
|---|---|---|
| `windowWait` | admission of the window's last price event | the first admission, on any partition, of a price event at or after `windowEnd`. This is the rest of the five-minute window, which no setting can shorten |
| `boundWait` | the end of `windowWait` | the first admission of a price event at or after `windowEnd` plus the bound |
| `alignmentWait` | the end of `boundWait` | modelled readiness: every partition that is not idle has had such an event, or the quiet ones have gone idle |
| `queueing` | modelled readiness | emission. Broker append, fetch, the source, the periodic watermark emission (200 ms by default), the idleness check, network buffers and `q1-ema` itself. These cannot be told apart with these timestamps |
| `sinkHandoff` | emission | the output record's timestamp: the shuffle to `subscription-filter`, the filter and serialization, up to the sink's `send()` |

`watermarkWait` is `boundWait` plus `alignmentWait`. `emitLatency` is admission to emission and `outputLatency` admission to the output timestamp. Each stage reports n, mean, the nearest-rank p50, p95 and p99, and the maximum. Percentiles of the stages do not add up to the total's; the means do.

The model is the one in [interfaces.md](../docs/interfaces.md#event-time-late-events-and-end-of-input-d-5-d-38): a partition's watermark is its largest event time minus the bound minus 1 ms, a partition is idle after `idlenessMs` without a price event, and the job's watermark is the minimum over partitions that are not idle, never decreasing. A partition that has had no event counts its silence from when the job's tasks were all running (`times.jobRunning`). `queueing` is therefore the job's delay behind the model, and the model's error lands in it too. It is negative where the job's watermark passed the window before the model's, for example when Flink declared a partition idle earlier than the model did. `negativeQueueing` counts those results.

### Which results count

A result is measured when its last price event was admitted inside the measurement interval and the job emitted it before `flink stop --drain` began (`times.drainStart`). `excluded` counts the rest: `beforeInterval` (the warm-up, including the unpaced rows before `--pace-from`), `afterInterval` and `drain`, the windows the drain fired, among them the day's last windows that wait for input that never comes (D-38). `unmodelled` counts measured results whose window the model never released; they count in `emitLatency` and `outputLatency` but in no stage.

### Watermark lag

Prometheus reads `q1-ema`'s `currentInputWatermark` every 5 s, and `timestamp()` of the same series gives the instant of each read. At each read inside the interval:

- `eventMs`: the largest event time the feed had admitted, minus the watermark.
- `wallMs`: how long before the read the feed had first admitted the watermark's event time.
- `modelAheadEventMs`: the modelled watermark minus the job's, a check on the model.

Both lags include the bound: 1 s of event time is 10 ms of wall-clock time at 100x.

### Several hosts

The harness runs on one host. A run across hosts would put the replayer's clock in admission and a TaskManager's in emission, so every stage would carry their offset. Such a run has to record each host's offset and its error bound (`chronyc tracking`: system time offset and root dispersion) before and after, and a stage smaller than that bound cannot be measured. #12 owns those runs.

## Not covered

- Multi-VM runs, clock synchronization between hosts, and Docker Swarm (#12). [Several hosts](#several-hosts) says what latency would need.
- State bytes per subtask: the harness keeps `flink_jobmanager_job_lastCheckpointFullSize` when checkpointing is on, but not the REST checkpoint details.
- Publication time: the broker's append of output records and the moment a consumer sees them.
