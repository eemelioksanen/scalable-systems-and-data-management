# Watermark bound, idleness and end of input (#7)

The runs behind D-38 in [decisions.md](../../decisions.md): which events the job rejects as late under each bound and idleness timeout, in a preload and in a paced replay, and whether its output then equals the oracle's.

**Correctness evidence, not performance.** Up to three stacks ran on the host at once, so the timings here describe the watermark, not the system's speed.

## Setup

- Host: Intel i7-11700K (16 threads, 62.5 GiB), Arch Linux, Docker 29.8.2 with Compose 5.6.0. These are the stack's first runs on Docker for Linux; the earlier ones used Podman.
- Code: `843c89f` has the job as on main at `62ed09c`, whose defaults were 60 s and 30 s in both modes, plus the Kafka image fix. `594761f` adds D-38's defaults and the end-of-input change. Every harness run states its bound and idleness, so the defaults only matter in the run without flags.
- Stack: 8 partitions, one TaskManager, one `*` subscription. Parallelism 4 for every symbol, 2 for the 12 symbols.
- Inputs: the full week in `data/daily/` (289,116,808 rows, 59,391,670 price events), its first three days (177,398,268 rows, 35,762,914 price events), and Monday of `data/subset12/` (4,216,881 rows, 148,465 price events of 12 symbols, on 6 of the 8 partitions).
- References: oracle runs on the same files. `week` and `subset12-mon` admit every event. `mon-wed-bound60s` and `mon-wed-bound1s` reject the 5,506 burst rows and have identical output: 1,116,608 results and 6,607 advisories, against 1,118,420 and 6,608 with the burst.
- Harness: [bench/run.py](../../../bench/README.md) with the configs in [bench/configs/watermark/](../../../bench/configs/watermark/). Every run is drained, so its `lateTicks` is final. A live run starts the job, then replays paced from Monday 09:00; the 745,472 rows before that (7,375 in the subset) go unpaced.

[tables.py](tables.py) rebuilds [runs.csv](runs.csv) from the retained runs and copies each run's `config.json` and `summary.json` into `runs/`. A run is `COMPLETE` when its counts reconcile and the comparison passes, and `INVALID` when the comparison finds differences or a late count other than the oracle's.

## Finite replay: the full week, preloaded

Compared with the admit-all output. The job read the preloaded topic and was then drained.

| Revision | Bound | Idleness | `lateTicks` | Missing windows | EMA differences | Advisory differences | Verdict |
|---|---|---|---|---|---|---|---|
| `843c89f` | 64,265,001 ms | off | 0 | 0 | 0 | 0 | pass |
| `594761f` | 64,265,001 ms | off | 0 | 0 | 0 | 0 | pass |
| `594761f`, `--bounded` without flags | default | default | 0 | 0 | 0 | 0 | pass |
| `843c89f` | 60 s | 30 s | 5,506 | 1,812 | 686,473 | 838 | fail |
| `843c89f` | 60 s | 30 s | 5,506 | 1,812 | 686,473 | 838 | fail |
| `843c89f` | 60 s | 30 s | 4,044 | 1,332 | 504,664 | 642 | fail |
| `843c89f` | 1 s | 30 s | 5,506 | 1,812 | 686,473 | 838 | fail |
| `843c89f` | 1 s | 30 s | 5,506 | 1,812 | 686,473 | 838 | fail |
| `843c89f` | 1 s | 30 s | 5,506 | 1,812 | 686,473 | 838 | fail |

- At the all-admitting bound the job published 1,857,457 results and 11,355 advisories, all equal to the oracle's. Up to 623,030 windows were open at once, summing each subtask's highest sample.
- EMA differences include the missing windows. 686,473 is D-30's 684,661 changed windows plus its 1,812 missing ones, and 838 its 422 removed and 416 added advisories.
- The third 60 s run admitted 1,462 of the burst rows. A short bound in a preload rejects whatever the watermark has already passed, and how far the watermark trails the reads differs from run to run.
- The run without flags is `runs/week-bounded-defaults-594761f/`: the `594761f` jar submitted with only `--bounded` to the stack of the last preload, writing to fresh output topics. A bounded job's counters vanish with it, so `lateTicks` was sampled from the TaskManager every 200 ms. The last sample is final: it has all 59,391,670 price events received and 0 late.

## Live: every symbol, Monday to Wednesday

Compared with the oracle at the job's bound, which rejects 5,506 events. The replay crosses two nights and ends after the burst.

| Revision | Rate | Bound | Idleness | `lateTicks` | Missing windows | EMA differences | Verdict | Send lag p99 / max | Last ingest to emit, p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|---|---|
| `843c89f` | 300× | 60 s | 30 s | 5,506 | 0 | 0 | pass | 3.2 / 3.6 s | 0.41 / 0.96 / 1.36 s |
| `843c89f` | 300× | 1 s | 30 s | 5,506 | 0 | 0 | pass | 8.3 / 8.6 s | 0.24 / 0.74 / 1.13 s |
| `843c89f` | 300× | 1 s | 5 s | 6,106 | 37 | 4,027 | fail | 36.0 / 36.2 s | 0.28 / 0.78 / 1.20 s |
| `594761f` | 100× | 1 s | 30 s | 5,506 | 0 | 0 | pass | 0.01 / 0.4 s | 0.38 / 1.99 / 3.17 s |

- With 30 s idleness the job rejected exactly the burst and published the oracle's 1,116,608 results and 6,607 advisories, at both bounds and both rates. Neither night lost a tick.
- With 5 s idleness it rejected 600 more. The job had been running for 19.8 s when the replay started, so every partition was idle, and the unpaced rows before 09:00 raced: 10 FR indices lost their first three windows of Monday, 07:05 to 07:15, and every later EMA of theirs differs: 3,990 windows. The other 7 missing windows are the last ones of Wednesday, lost at the drain (below).
- The send lag is how far the replayer fell behind its schedule. The 300× runs shared the host with two other stacks; the 100× run had it to itself. The 36 s stall of the third run left no difference outside the two places above.
- Last ingest to emit is `emitTs - maxIngestTs` over the results published before the replay ended. It includes the wait from a symbol's last trade to the end of its window. Its maximum is a night, 102 s at 300× and 306 s at 100×, or 8.5 h: the windows after the day's last price event are published when the next day's first events arrive.

## Live: 12 symbols, Monday

Compared with the admit-all output: these symbols have no late event on Monday at any bound. Two of the 8 partitions are empty and the others are sparse.

| Revision | Bound | Idleness | `lateTicks` | Results of 1,388 | Verdict | Last ingest to emit, p50 / p95 / p99 / max |
|---|---|---|---|---|---|---|
| `843c89f` | 60 s | 30 s | 0 | 1,388 | pass | 1.00 / 15.91 / 30.45 / 32.65 s |
| `843c89f` | 1 s | 30 s | 0 | 1,388 | pass | 0.44 / 16.02 / 30.38 / 30.78 s |
| `843c89f` | 60 s | 5 s | 0 | 1,384 | fail | 0.96 / 1.50 / 5.40 / 7.65 s |
| `843c89f` | 1 s | 5 s | 0 | 1,386 | fail | 0.42 / 0.97 / 5.39 / 5.79 s |
| `843c89f` | 1 s | 1 s | 0 | 1,386 | fail | 0.45 / 0.76 / 2.39 / 3.13 s |
| `594761f` | 1 s | 5 s | 0 | 1,388 | pass | 0.42 / 0.88 / 5.32 / 5.71 s |

- At 100× the bound costs 0.6 s of wall-clock time at 60 s and 0.01 s at 1 s, which is the difference between the two p50s. The tail is the idleness timeout: a window waits for every partition to pass its end or go idle.
- No run rejected a tick, so at 100× a 1 s bound was enough even with partitions going idle and resuming all day.
- The three failures at `843c89f` are windows missing with `lateTicks` 0, all of them the last window of a symbol.

## End of input with idle sources

Flink ends event time from the sources. At the end of a bounded input and on `flink stop --drain`, each source task sends the maximum watermark (`SourceOperatorStreamTask.advanceToEndOfEventTime`). The task's output drops any watermark while it is idle (`RecordWriterOutput.emitWatermark`), and nothing marks it active first. If every source subtask is idle, no operator learns that event time has ended, and the windows still open are dropped without being published or counted.

The harness drains a live run a few seconds after the job has read the last record: 9 to 16 s after the replay ended in these runs. With 30 s idleness the sources were still active then. With 5 s or 1 s they were idle, and the last windows were lost.

`594761f` makes `q1-ema` end event time itself when its input ends (`EndOfInputProcessOperator`). `TradingTrendsJobTest.endOfInputEvaluatesEveryWindowAlthoughTheSourceIsIdle` reproduces the loss on a bounded input: without the change the job publishes nothing.

## Reproducing

```bash
# The 12-symbol subset is described in bench/README.md.
D=data/daily/debs2022-gc-trading-day
oracle/run.sh --out /out/week $D-*.csv
oracle/run.sh --out /out/mon-wed-bound60s --watermark-bound-ms 60000 $D-08-11-21.csv $D-09-11-21.csv $D-10-11-21.csv
oracle/run.sh --out /out/mon-wed-bound1s --watermark-bound-ms 1000 $D-08-11-21.csv $D-09-11-21.csv $D-10-11-21.csv
oracle/run.sh --out /out/subset12-mon data/subset12/debs2022-gc-trading-day-08-11-21.csv

for config in bench/configs/watermark/*.json; do python3 bench/run.py "$config"; done
bench/.venv/bin/python docs/validation/2026-10-13-watermark/tables.py
```

To run several stacks side by side, give each its own `COMPOSE_PROJECT_NAME`, `COMPOSE_NETWORK` (`<project>_default`) and host ports ([README](../../../README.md#quickstart)).
