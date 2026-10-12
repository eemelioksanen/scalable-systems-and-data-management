# Local baselines and bottlenecks (#24)

**Tuning data, not results.** Every run here is single and unrepeated, on one laptop VM, and the full-week correctness gate (#9) has not passed. Use these numbers to plan #11 and #12, not to describe the system's performance. The decisions taken from them are D-37 in [decisions.md](../../decisions.md).

## Setup

- Host: Apple M4 Pro (14 cores, 48 GiB), running a rootless Podman 6.1.3 VM with 7 vCPUs and 8 GiB. Code at revision `2c89b5b`, the harness from D-36.
- Input: the full week, `data/daily/` (289,116,808 rows), or 08 November alone (53,994,554 rows) for the live runs.
- Preload runs: `--bounded`, bound 64,265,001 ms and idleness off (admit-all, D-30), so no event is late and every run has the same output.
- Live runs: the provisional 60 s bound and 30 s idleness (D-5), paced from 09:00, finished with a drain.
- One `*` subscription. The dashboard (`viz`) also runs and reads `ema-results`; it used at most 0.33 cores.

The configs are in [bench/configs/baseline/](../../../bench/configs/baseline/). Each `runs/<name>/` keeps that run's `config.json` (stack, hardware, versions, timestamps) and `summary.json`. [runs.csv](runs.csv) is `summarize.py` over all 13, and [subtask-load.csv](subtask-load.csv) compares each preload's subtask loads with [bench/predict_load.py](../../../bench/predict_load.py).

Two local workarounds were needed under rootless Podman, and neither changes the stack under test: `COMPOSE_FILE` added an override that gives cAdvisor the Podman socket (D-35), and `docker` was a wrapper that turns `correctness/run.sh`'s `--user uid:gid` into `--userns=keep-id`, because rootless Podman cannot map this Mac's group ID.

## Runs

| Run | Partitions | TMs x slots | Parallelism | Replay M rec/s | Job M rec/s | Busy: source / q1 / output | TM cores (max) | Host cores (max) | Kafka cores (max) |
|---|---|---|---|---|---|---|---|---|---|
| week-preload-p1 | 8 | 1 x 2 | 1 | 1.56 | 4.93 | 0.85 / 0.52 / 0.08 | 3.7 | 4.3 | 0.45 |
| week-preload-p2 | 8 | 1 x 2 | 2 | 1.59 | 6.87 | 0.83 / 0.50 / 0.09 | 5.0 | 5.6 | 0.55 |
| week-preload-p4 | 8 | 1 x 4 | 4 | 1.56 | 8.05 | 0.72 / 0.45 / 0.09 | 5.5 | 6.1 | 0.46 |
| week-preload-p4-2tm | 8 | 2 x 2 | 4 | 1.55 | 8.08 | 0.71 / 0.47 / 0.09 | 5.7 | 6.1 | 0.48 |
| week-preload-p4-16part | 16 | 1 x 4 | 4 | 1.44 | 7.26 | 0.79 / 0.41 / 0.09 | 5.5 | 6.1 | 0.45 |
| week-preload-p6 | 8 | 1 x 6 | 6 | 1.56 | 6.93 | 0.77 / 0.34 / 0.08 | 5.7 | 6.3 | 0.50 |
| week-preload-p6-16part | 16 | 1 x 6 | 6 | 1.45 | 6.70 | 0.78 / 0.36 / 0.08 | 5.7 | 6.3 | 0.46 |
| week-preload-p8 | 8 | 1 x 8 | 8 | 1.52 | 7.49 | 0.75 / 0.35 / 0.09 | 5.6 | 6.4 | 0.49 |
| week-preload-p4-fr | 8 | 1 x 4 | 4 | 1.10 | 8.11 | 0.69 / 0.38 / 0.13 | 5.3 | 5.9 | 0.50 |
| week-preload-p4-fr-nl | 8 | 1 x 4 | 4 | 1.41 | 9.19 | 0.69 / 0.35 / 0.10 | 5.3 | 5.9 | 0.48 |

Job throughput is the records the `ticks` source read over its subtasks' span (D-36). Busy ratios are over the tasks' whole run. CPU maxima are the highest 5 s samples over the job's run, from `summary.json`; for two TaskManagers, the sum of each one's maximum. The replay rate of an exchange subset counts only the rows it sent, though it reads every row.

Every run ended `COMPLETE` and reconciled. Each all-symbol preload exported 1,857,457 results and 11,355 advisories, the oracle's full-week counts, though no run here was compared with the oracle. FR alone gave 158,331,078 records and FR with NL 238,322,268.

| Live run, 08 November from 09:00 | Rate achieved | Send lag p50 / p99 / max | Into Kafka, peak | Job lag, max | Source backpressure, max | TM cores, max |
|---|---|---|---|---|---|---|
| day-live-100x | 100.0 | 1 / 5 / 39 ms | 256 k rec/s | 1,541 records at the first scrape, then at most 259 | 7 ms/s | 0.95 |
| day-live-300x | 300.0 | 0 / 21 / 181 ms | 702 k rec/s | 253 records at the first scrape, then 0 | 54 ms/s | 1.28 |
| day-live-1000x | 999.98 | 2.4 / 4.7 / 4.8 s | 1.51 M rec/s | 211 records | 3 ms/s | 1.52 |

All three exported 373,126 results and 4,840 advisories, the oracle's counts for the day, with `lateTicks` 0. [timeseries-live.png](timeseries-live.png) shows the read rate and lag.

A dry run of the replayer over the week, reading and projecting without Kafka, took 87 s: 3.32 M rows/s ([replayer-dry-run.json](replayer-dry-run.json)).

## Bottlenecks

**Replay.** The single replayer limits every input rate. It preloads at 1.44 to 1.59 M records/s, using 1.06 to 1.21 cores, while Kafka uses about 0.33 cores, the broker's request handlers are at least 98.2% idle on average and the VM is at about 1.5 of its 7 cores ([preload-phase.csv](preload-phase.csv), from 20 s into each preload). Without Kafka it reads and projects 3.32 M rows/s. So its own producer path, not the files, the broker or the CPU, sets the rate. At 1000x it reaches the same 1.51 M records/s and falls up to 4.8 s behind its schedule.

**Kafka.** Not a bottleneck anywhere. While the job read a preload at up to 9 M records/s, the broker used at most 0.55 cores and its request handlers were at least 99% idle. Taking 1.5 M records/s from the replayer at 1000x, it used up to 1.16 cores, with its handlers 97.8% idle on average.

**Flink.** The job is CPU-bound from parallelism 4. Throughput rises 1.39x from p1 to p2 and 1.17x from p2 to p4, then falls: p6 and p8 are slower than p4. From p4 the VM uses 6.1 to 6.4 of its 7 cores, 5.5 of them in the TaskManager. Splitting p4 over two TaskManagers on the same VM changes nothing (8.08 against 8.05 M records/s). The `ticks` source is the busiest stage in every run, busy 0.69 to 0.85, against 0.34 to 0.52 for `q1-ema`. So the source, which parses every raw row, is the stage to make cheaper first; #23's early-filtering and binary-encoding variants target it. Even at p1 the TaskManager used up to 3.7 cores, for the source, its Kafka fetcher, `q1-ema`, the output and GC. In a live replay the job keeps up at every rate the replayer can offer: after the first scrape its lag was never above 259 records.

**Output.** Not a bottleneck. The subscription filter and both sinks were busy 0.08 to 0.13.

So locally the job's ceiling is about 8 M records/s, set by the VM's CPU, and the replayer's is about 1.5 M records/s. A live run cannot load the job to saturation with one replayer.

## Skew and partitions

`bench/predict_load.py` matched the measured records of every source subtask and the price events of every `q1-ema` subtask exactly, in all ten preloads ([subtask-load.csv](subtask-load.csv)). Two things follow, both visible in [predicted-load.csv](predicted-load.csv):

- When the parallelism divides the partition count, each source subtask reads the symbols whose Kafka hash modulo the parallelism is its index. The partition count then has no effect: 8, 16 and 32 partitions give the same loads at p2, p4 and p8, and 16 and 32 the same at p16. The hash alone sets the skew: 1.05 at p4, 1.25 at p8 and 1.49 at p16 in raw records.
- When it does not divide, some subtasks read more partitions. At p6, 8 partitions give a source skew of 1.72, and 16 partitions give 1.33. With 8 partitions, p16 leaves half its source subtasks without a partition.

`q1-ema`'s skew follows Flink's key groups, independent of partitions: 1.02 at p2, 1.10 at p4, 1.15 at p8 and 1.27 at p16, in price events.

Locally the skew did not limit throughput, because the CPU limit came first: p6 ran at about the same speed with skew 1.72 and 1.33. p4 at 16 partitions was 10% slower than at 8 with identical loads, which a single run cannot tell apart from run-to-run variation; #11's repeats will.

## Not feasible locally

| Configuration | Why |
|---|---|
| Parallelism above 4 | The VM's 7 cores are saturated at p4; p6 and p8 were slower. |
| Horizontal scaling (more TaskManagers) | All TaskManagers share the same VM; 2 x 2 slots equals 1 x 4. |
| p16 | Needs at least 16 partitions and more than 7 cores. |
| Input rates above about 1.5 M records/s | One replayer's ceiling. Needs several replayers, which the harness does not run. |
| Full-day live replay at 1x or 10x | 13.4 h and 80 min of wall time from 09:00; feasible, but only slices (`--limit`) are practical. |
| Full-week live replay | A week of wall time at 1x, and the 30 s idleness drops ticks after long gaps (D-28). |

## Run procedure

Per run, an all-symbol full-week preload took 5.5 to 6 minutes: 11 to 21 s of reset, 182 to 201 s of preload, 36 to 59 s of job and about a minute of collection and export. The read rate reached its steady value by the second scrape, about 10 s after the job started; whole-job throughput was within 5% of the steady-state rate in most runs, but that rate rests on at most five scrapes. In a live replay the unpaced rows before 09:00 (745,472 of them on 08 November) were read within the first scrape, after which lag stayed under 260 records. D-37 fixes the procedure from these figures.
