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
  bench/results/raw/subset12-preload-p2-<stamp> --out bench/results/parallelism
```

`run.py` builds the images first unless given `--no-build`, and `--repeats N` overrides the config. It exits nonzero if any repeat failed. It takes over the Compose project `trading-trends`: every repeat runs `docker compose down --volumes`, so stop anything else using that stack first. The stack stays up after the last repeat for inspection.

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
| `job` | `watermarkBoundMs`, `idlenessMs` and `zoneOffset`, all required, and optional extra `args`. The bound and idleness are still open (D-5, #7), so the harness has no defaults for them. |
| `stack` | `partitions` (`TICKS_PARTITIONS`), `taskmanagers` (`--scale`), `slots` (`TM_SLOTS`), `parallelism` (`PARALLELISM`). Defaults 8, 1, 2, 2. |
| `checkpointing` | `true` submits with `-Dexecution.checkpointing.interval=10s -Drestart-strategy.type=none`, which state bytes need (D-35). Default `false`. |
| `subscriptions` | Trader records written to `subscriptions` before the job starts. Default `[{"trader":"all","symbols":["*"]}]`. |
| `repeats`, `warmupS`, `timeoutS` | Repeats; seconds excluded from the start of the measurement interval; how long any wait may take (default 3600). |
| `oracle` | An oracle output directory for the correctness check. The harness refuses one computed from other files than `input`. |

## What a run does

1. **Reset.** `docker compose down --volumes`, then `up -d --scale taskmanager=N`. It waits for `kafka-init` to exit, Flink to register the TaskManagers and slots, and Prometheus to scrape every Flink, Kafka and cAdvisor target.
2. **Subscriptions** are written before the job is submitted, so the output is deterministic (D-28).
3. **Replay and job.** A preload replays everything with the job stopped, then submits it. A live run submits the job, waits for all its tasks to run, then replays at the configured rate.
4. **Completion.** A bounded job is done when Flink reports it `FINISHED`. A drained job is done once the sum of the job's Kafka offsets reaches the replay's end offsets and q1-ema has received every price event, by Prometheus; the harness waits one more scrape, then stops it with `--drain`.
5. **Collection**, before the next reset deletes it: Flink's record of the job, Prometheus range queries, the job's counters, the effective configuration, and an export of `ema-results` and `advisories` with `correctness/run.sh export`. With an `oracle`, `correctness/run.sh compare` runs too.

The **measurement interval** is the job's run in a preload and the replay in a live run, both minus `warmupS` at the start. It bounds the metric summaries and the latency figures. Preload time is never inside it: it is the replayer's own `durationMs` and `recordsPerSecond`, kept separately (D-35: a short preload gives Prometheus two or three scrapes).

## Output

Each repeat writes `bench/results/raw/<name>-<stamp>/r<k>/`. The run ID is `<name>-<stamp>-r<k>`, and the replayer's report carries it too.

| File | Contents |
|---|---|
| `config.json` | The config record: the experiment config, the code revision and whether the tree was dirty, input files with size and MD5, checkpointing, the job's submitted arguments and ID, host hardware, the container engine's `info`, every container's image ID and CPU and memory limits (0 = none), Flink's TaskManagers with their slots and memory, Flink's configuration, the JobManager's clock offset from the host, and the phase timestamps. Written even when a run fails, with its error. |
| `compose.yml` | `docker compose config` for this run: the stack as Compose resolved it. |
| `replay.json` | The replayer's report: status, counts, offsets per partition, timing, pacing. |
| `flink-job.json` | Flink's REST view of the finished job, per vertex and subtask: records and bytes in and out, busy, idle and backpressured time. Flink keeps it after the job ends, so these counts are exact. |
| `metrics.json` | Every D-35 query as a 5 s range query over the run, raw. |
| `export/` | The correctness export: `ema_results.parquet`, `advisories.parquet` (with `maxIngestTs` and `emitTs`) and its `manifest.json`. |
| `compare/` | With an `oracle`: the comparison report and differences ([correctness/README.md](../correctness/README.md)). |
| `summary.json` | What the run derived: throughput, per-subtask load and skew, the metric summaries over the measurement interval, the counters, the reconciliation and the correctness result. |
| `commands.log` | Every command and its stderr. |

`summarize.py` writes into the experiment directory, or `--out`:

| File | Contents |
|---|---|
| `runs.csv` | One row per run: stack size, replay and job throughput, subtask skew and busy ratios, output counts, `lateTicks`, reconciliation and correctness results, and in live runs the last-ingest-to-emit percentiles. |
| `summary.csv` | Per experiment and metric: n, mean, sample standard deviation, min and max, over the runs that completed. |
| `throughput.png` | Job and replayer records per second: mean, standard deviation and each run. |
| `timeseries.png` | Ticks read per second and lag over time, one line per repeat. |
| `latency.png` | Live runs only: last-ingest-to-emit p50, p95 and p99. |

## Counts and reconciliation

A bounded job's custom counters vanish when it finishes, and on a short run Prometheus never scrapes them: a 13.8 M-record preload is read in about 3.5 s. Flink's REST record of the job keeps each vertex's record counts, so reconciliation uses those:

- the replay's acknowledged records = records the `ticks` source read
- price events the source emitted = records q1-ema received
- records the sink chain wrote = records exported from both topics
- with a `*` subscription, windows q1-ema evaluated = exported `ema-results`
- with an oracle, replayed data rows and price events equal the oracle's

The counters (interfaces.md, "Counts") come from Prometheus's last sample. `countersFinal` says which of them are exact: a counter is final when its last sample agrees with the job graph. The source's split and `lateTicks` are final in drained runs and seldom in bounded ones, so `compare` gets `--job-late-ticks` only when it is final. Without it the gate fails with "lateTicks was not given", even when the export has no differences. For the correctness gate, use `"finish": "drain"`. `windows` and `published` are not final in a drained run, because the drain fires windows after the last scrape: use the job graph's counts for them.

## Latency

Live runs keep every result's `maxIngestTs` and `emitTs`, and `summarize.py` reports `emitTs - maxIngestTs`, from the window's last contributing price event entering Kafka to the job emitting the result, over results emitted within the measurement interval. It includes the watermark wait, so it is not the processing latency. How to separate watermark waiting from queueing and processing, and what to report, waits for the bound and idleness decision in #7 (D-36).

## Not covered

- Multi-VM runs, clock synchronization between hosts, and Docker Swarm (#12).
- State bytes per subtask: the harness keeps `flink_jobmanager_job_lastCheckpointFullSize` when checkpointing is on, but not the REST checkpoint details.
- Publication time: the export does not keep the output records' Kafka timestamps.
