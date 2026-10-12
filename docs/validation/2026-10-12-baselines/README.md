# Local baselines and bottlenecks (#24)

**Tuning data, not results.** Every run here is single and unrepeated, on one laptop VM, and the full-week correctness gate (#9) has not passed. Use these numbers to plan #11 and #12, not to describe the system's performance. The decisions taken from them are D-37 in [decisions.md](../../decisions.md).

## Setup

- Host: Apple M4 Pro (14 cores, 48 GiB), running a rootless Podman 6.1.3 VM with 7 vCPUs and 8 GiB. Code at revision `2c89b5b`, the harness from D-36.
- Input: the full week, `data/daily/` (289,116,808 rows), or 08 November (53,994,554 rows) for the live runs.
- Preload runs: `--bounded`, bound 64,265,001 ms and idleness off (admit-all, D-30), so no event is late and every run has the same output.
- Live runs: the provisional 60 s bound and 30 s idleness (D-5), paced from 09:00, finished with a drain. The 1x and 10x runs replay only the first 3,000,000 rows, to 09:16:42.
- One `*` subscription. The dashboard (`viz`) also runs and reads `ema-results`; it used at most 0.33 cores.

The configs are in [bench/configs/baseline/](../../../bench/configs/baseline/). Each `runs/<name>/` keeps that run's `config.json` (stack, hardware, versions, timestamps) and `summary.json`. [tables.py](tables.py) rebuilds every table here from the retained runs: [runs.csv](runs.csv) (`summarize.py` over all runs), [subtask-load.csv](subtask-load.csv) (each preload's subtask records against [bench/predict_load.py](../../../bench/predict_load.py)), [preload-phase.csv](preload-phase.csv) (the replayer and the broker during each preload) and [predicted-load.csv](predicted-load.csv) (`predict_load.py` with its defaults).

Two local workarounds were needed under rootless Podman, and neither changes the stack under test. `COMPOSE_FILE` added an override that gives cAdvisor the Podman socket (D-35). `docker` was a wrapper that turns `correctness/run.sh`'s `--user uid:gid` into `--userns=keep-id`, because rootless Podman cannot map this Mac's group ID; a first p2 run failed at its export on exactly that and was rerun.

## Runs

| Run | Partitions | TMs x slots | Parallelism | Replay M rec/s | Job M rec/s | Busy: source / q1 / output | Source busy skew | TM cores (max) | Host cores (max) | Kafka cores (max) |
|---|---|---|---|---|---|---|---|---|---|---|
| week-preload-p1 | 8 | 1 x 2 | 1 | 1.56 | 4.93 | 0.85 / 0.52 / 0.08 | 1.00 | 3.7 | 4.3 | 0.45 |
| week-preload-p2 | 8 | 1 x 2 | 2 | 1.59 | 6.87 | 0.83 / 0.50 / 0.09 | 1.02 | 5.0 | 5.6 | 0.55 |
| week-preload-p4 | 8 | 1 x 4 | 4 | 1.56 | 8.05 | 0.72 / 0.45 / 0.09 | 1.03 | 5.5 | 6.1 | 0.46 |
| week-preload-p4-2tm | 8 | 2 x 2 | 4 | 1.55 | 8.08 | 0.71 / 0.47 / 0.09 | 1.04 | 5.7 | 6.1 | 0.48 |
| week-preload-p4-16part | 16 | 1 x 4 | 4 | 1.44 | 7.26 | 0.79 / 0.41 / 0.09 | 1.04 | 5.5 | 6.1 | 0.45 |
| week-preload-p6 | 8 | 1 x 6 | 6 | 1.56 | 6.93 | 0.77 / 0.34 / 0.08 | 1.39 | 5.7 | 6.3 | 0.50 |
| week-preload-p6-16part | 16 | 1 x 6 | 6 | 1.45 | 6.70 | 0.78 / 0.36 / 0.08 | 1.21 | 5.7 | 6.3 | 0.46 |
| week-preload-p8 | 8 | 1 x 8 | 8 | 1.52 | 7.49 | 0.75 / 0.35 / 0.09 | 1.11 | 5.6 | 6.4 | 0.49 |
| week-preload-p4-fr | 8 | 1 x 4 | 4 | 1.10 | 8.11 | 0.69 / 0.38 / 0.13 | 1.07 | 5.3 | 5.9 | 0.50 |
| week-preload-p4-fr-nl | 8 | 1 x 4 | 4 | 1.41 | 9.19 | 0.69 / 0.35 / 0.10 | 1.03 | 5.3 | 5.9 | 0.48 |

Job throughput is the records the `ticks` source read over its subtasks' span (D-36). Busy ratios are over the tasks' whole run. Busy skew is the busiest source subtask's busy time over the mean. CPU maxima are the highest of the 15 s rates Prometheus scraped every 5 s during the job, from `summary.json`; for two TaskManagers they are the sum of each one's maximum, an upper bound. The replay rate of an exchange subset counts only the rows it sent, though it reads every row.

Every run ended `COMPLETE` and reconciled. Each all-symbol preload exported 1,857,457 results and 11,355 advisories, the oracle's full-week counts, though no run here was compared with the oracle. FR alone gave 158,331,078 records and FR with NL 238,322,268.

| Live run, 08 November from 09:00 | Rate achieved | Send lag p50 / p99 / max | Into Kafka, peak | Job lag, max | Source backpressure, max (sum over subtasks) | TM cores, max |
|---|---|---|---|---|---|---|
| day-live-1x-slice | 1.0 | 2 / 13 / 43 ms | 5.8 k rec/s | 0 | 9 ms/s | 0.43 |
| day-live-10x-slice | 10.0 | 2 / 11 / 18 ms | 31 k rec/s | 0 | 4 ms/s | 0.91 |
| day-live-100x | 100.0 | 1 / 5 / 39 ms | 256 k rec/s | 1,541 records at the first scrape, then at most 259 | 7 ms/s | 0.95 |
| day-live-300x | 300.0 | 0 / 21 / 181 ms | 702 k rec/s | 253 records at the first scrape, then 0 | 54 ms/s | 1.28 |
| day-live-1000x | 999.98 | 2.4 / 4.7 / 4.8 s | 1.51 M rec/s | 211 records | 3 ms/s | 1.52 |

The three whole-day runs exported 373,126 results and 4,840 advisories, the oracle's counts for the day, with `lateTicks` 0. The slices have no oracle reference, since the oracle reads whole files. Both reconciled with `lateTicks` 0 and exported the same 39,073 results and 3,836 advisories. [timeseries-live.png](timeseries-live.png) shows the read rate and lag.

A dry run of the replayer over the week, reading and projecting without Kafka, took 87 s: 3.32 M rows/s ([replayer-dry-run.json](replayer-dry-run.json)).

## Bottlenecks

**Replay.** The single replayer limits every input rate. It preloads at 1.44 to 1.59 M records/s, using 1.06 to 1.21 cores, while Kafka uses about 0.33 cores, the broker's request handlers are at least 98.2% idle on average, and the VM is at about 1.5 of its 7 cores ([preload-phase.csv](preload-phase.csv)). Without Kafka it reads and projects 3.32 M rows/s. So publishing to Kafka halves its rate, and with the broker and the VM mostly idle, the limit is most likely the replayer's one sending thread (D-31). These runs do not separate that thread's own work from the producer's compression, batching or network waits. At 1000x it reaches the same 1.51 M records/s and falls up to 4.8 s behind its schedule.

**Kafka broker.** Not a bottleneck anywhere. While the job read a preload at up to 9 M records/s, the broker used at most 0.55 cores and its request handlers were at least 99% idle. Taking 1.5 M records/s from the replayer at 1000x, it used up to 1.16 cores, with its handlers 97.8% idle on average. The consumer side, inside the TaskManager, is part of the Flink figures below.

**Flink.** Throughput rises 1.39x from p1 to p2 and 1.17x from p2 to p4, then stops rising: p6 and p8 read 6.93 and 7.49 M records/s against p4's 8.05 M, differences a single run cannot rank. Splitting p4 over two TaskManagers on the same VM changes nothing (8.08 M records/s).

The likeliest limit is the VM's CPU. From p4 on, the VM holds at 5.8 to 6.4 of its 7 cores through most of each job, up to 5.7 of them in the TaskManager, while the broker and the output idle. Skew does not explain it at p4 (source busy skew 1.03). At p6 it adds to it: 8 partitions give a busy skew of 1.39 and 16 partitions 1.21, at about the same speed.

No task thread is saturated, though. The `ticks` source is the busiest stage in every run, busy 0.69 to 0.85, against 0.34 to 0.52 for `q1-ema`. From p4 the source's aggregate idle time is 16 to 25%, waiting for records from the Kafka fetcher threads, whose CPU, lz4 decompression included, no busy metric counts. At p1 the TaskManager uses up to 3.7 cores while the source task is busy 0.85 and `q1-ema` 0.52, so much of its CPU goes to threads outside the measured tasks. Which work dominates needs a CPU profile of the TaskManager, which these runs did not take.

In a live replay the job keeps up at every rate the replayer can offer: after the first scrape its lag was never above 259 records.

**Output.** Not a bottleneck. The subscription filter and both sinks were busy 0.08 to 0.13.

So locally a full-week preload tops out at about 8 M records/s, most likely on the VM's CPU, and the replayer at about 1.5 M records/s. A live run cannot load the job to saturation with one replayer.

## Skew, partitions and state

`bench/predict_load.py` matched the measured records of every source subtask and the price events of every `q1-ema` subtask exactly, in all ten preloads ([subtask-load.csv](subtask-load.csv)). Two things follow, both visible in [predicted-load.csv](predicted-load.csv):

- When the parallelism divides the partition count, which source subtask reads a symbol depends only on its Kafka hash modulo the parallelism, plus a fixed rotation from the topic name. The record counts per subtask then do not depend on the partition count: 8, 16 and 32 partitions give the same counts at p2, p4 and p8, and 16 and 32 at p16. The hash alone sets the skew: 1.05 at p4, 1.25 at p8 and 1.49 at p16 in raw records. Processing cost per partition can still differ: p4 on 16 partitions read 7.26 M records/s against 8.05 M on 8, which one run each cannot tell apart from run-to-run variation.
- When it does not divide, some subtasks read more partitions. At p6, 8 partitions give a source skew of 1.72 in records and 16 partitions 1.33. With 8 partitions, p16 leaves half its source subtasks without a partition.

`q1-ema`'s skew follows Flink's key groups, independent of partitions: 1.02 at p2, 1.10 at p4, 1.15 at p8 and 1.27 at p16, in price events. Its busy skew was 1.07 at p4 and 1.11 at p8.

State per `q1-ema` subtask follows the same split: 1,194 to 1,259 symbols with EMAs per subtask at p4, of 4,942 at p1. Open windows peaked at 643,457 at p1 and 135,934 to 146,153 per subtask at p4, because a preload's watermark trails its reads (D-28).

## Left out of the local matrix

| Configuration | Why |
|---|---|
| Parallelism above 4 | p6 and p8 ran, but no faster than p4 with the VM near its 7 cores. |
| Horizontal scaling (more TaskManagers) | All TaskManagers share the same VM; 2 x 2 slots equals 1 x 4. |
| p16 | Needs at least 16 partitions and more than 7 cores. |
| Input rates above about 1.5 M records/s | One replayer's ceiling. Needs several replayers, which the harness does not run. |
| Whole-day live replay at 1x or 10x | 13.4 h and 80 min of wall time from 09:00. The slices above cover 09:00 to 09:16. |
| Full-week live replay | A week of wall time at 1x, and the 30 s idleness drops ticks after long gaps (D-28). |

## Run procedure

Per run, an all-symbol full-week preload took 5.5 to 6 minutes: 11 to 21 s of reset, 182 to 201 s of preload, 36 to 59 s of job and about a minute of collection and export.

The read rate steadied 13 to 19 s into each job. Whole-job throughput from Flink's REST record keeps that ramp on purpose, so it weighs more in a shorter job. Summing each source subtask's mean over the scrapes after the first, the later scrapes read 2 to 3% faster than the whole job at p1 and p2, within about 2% of it at p6, p8 and p4 on 16 partitions, 8 to 10% faster at p4 on 8 partitions, and 12 to 14% faster in the 20 to 26 s FR and FR-with-NL jobs, which have only one or two such scrapes.

`warmupS` changes only the Prometheus summaries, not Flink's records. In a preload no warm-up gives every run a clean steady state: a 15 s rate window sampled 15 s into a job still covers its start, and the FR job lasted only 20 s. In each live replay the job had read the unpaced rows before 09:00 (745,472 on 08 November) within 10 s of the replay's start, so a 25 s warm-up keeps every 15 s rate window clear of them. The baseline runs used 0 s for preloads and 10 s for live runs; D-37's values are not yet exercised.
