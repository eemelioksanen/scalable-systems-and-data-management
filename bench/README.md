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
| `stack` | `partitions` (`TICKS_PARTITIONS`), `taskmanagers` (`--scale`), `slots` (`TM_SLOTS`), `parallelism` (`PARALLELISM`). Defaults 8, 1, 2, 2. |
| `checkpointing` | `true` submits with `-Dexecution.checkpointing.interval=10s -Drestart-strategy.type=none`, which state bytes need (D-35). Default `false`. It needs `"finish": "drain"`: a bounded job would wait for a final checkpoint before it ends, and a few-second one takes none. |
| `subscriptions` | Trader records written to `subscriptions` before the job starts. Default `[{"trader":"all","symbols":["*"]}]`. |
| `repeats`, `warmupS`, `timeoutS` | Repeats; seconds excluded from the start of the measurement interval; how long the replay, the job and the drain may each take (default 3600). Startup waits have fixed limits of a few minutes. |
| `oracle` | An oracle output directory for the correctness check. The harness refuses one computed from other files than `input`, and one combined with `--limit`, `--symbols`, `--symbols-file` or `--exchanges`, since the oracle covers whole files. For a live run, use an oracle run at the job's bound: a live job rejects the burst that the admit-all output includes (D-38). |

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
| `metrics.json` | Every D-35 query as a 5 s range query over the run, raw. |
| `export/` | The correctness export: `ema_results.parquet`, `advisories.parquet` (with `maxIngestTs` and `emitTs`) and its `manifest.json`. |
| `compare/` | With an `oracle`: the comparison report and differences ([correctness/README.md](../correctness/README.md)). |
| `summary.json` | What the run derived: throughput, per-subtask load and skew, the metric summaries over the measurement interval, the counters, the reconciliation and the correctness verdict: `pass`, `unverified` (no differences, but `lateTicks` unknown) or `fail`. |
| `commands.log` | Every command and its stderr. |

`summarize.py` writes into the experiment directory, or `--out`:

| File | Contents |
|---|---|
| `runs.csv` | One row per run: status, stack size, replay and job throughput, subtask read skew (max over mean), busy ratios, output counts, `lateTicks` when final, reconciliation and correctness verdict. A busy ratio is 1 − (idle + backpressured) / duration over the tasks' whole run, from Flink's record of the job (D-35's definition of busy). The busy skew is the busiest subtask's busy time over the mean. CPU cores (the TaskManagers' summed, Kafka's, the replayer's and the host's) and the broker's request handler idle ratio are means over the measurement interval. |
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
- records the sink chain wrote = records exported from both topics
- with a `*` subscription, windows q1-ema evaluated = exported `ema-results`
- with an oracle, replayed data rows and price events equal the oracle's, and, when the source's counters are final, so do the non-price, non-positive and `Date`-dated events

The counters (interfaces.md, "Counts") come from Prometheus's last sample. `countersFinal` says whether the source's split and `lateTicks` are exact: the split when it adds up to the records the source read, `lateTicks` when q1-ema's last sampled input equals the records it received. Both are final in drained runs and seldom in bounded ones, so `compare` gets `--job-late-ticks` only when it is final; without it the verdict is at best `unverified`. For the correctness gate, use `"finish": "drain"`. The window, crossover and publication counters are never final in a drained run, because the drain fires windows after the last scrape: the job graph's counts replace them.

## Latency

Every run's export keeps each result's `maxIngestTs` (when the window's last contributing price event entered Kafka) and `emitTs` (when the job emitted it). The harness computes no latency from them yet. Their difference includes the watermark wait, which depends on the bound and idleness (D-38), and how to separate it from queueing and processing is open (D-36, #37).

## Not covered

- Multi-VM runs, clock synchronization between hosts, and Docker Swarm (#12).
- State bytes per subtask: the harness keeps `flink_jobmanager_job_lastCheckpointFullSize` when checkpointing is on, but not the REST checkpoint details.
- Publication time: the export does not keep the output records' Kafka timestamps.
