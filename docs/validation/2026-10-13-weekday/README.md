# Query 1 on a full weekday against the oracle (#21)

The runs behind D-39 in [decisions.md](../../decisions.md): Monday 08 November 2021 with every symbol, replayed from the CSV through Kafka and the Flink job into `ema-results`, and compared with the oracle by the comparer. They cover the watermark defaults of both modes, replay speeds from 10× to a preload, and input rewritten with bounded disorder.

**Correctness evidence, not performance.** A second stack ran the 10× replay on the same host throughout, and the injector and the oracle ran beside the first runs. The timings describe these runs, not the system's speed.

## Setup

- Host: Intel i7-11700K (16 threads, 62.5 GiB), Arch Linux, Docker 29.8.2 with Compose 5.6.0.
- Code: `af7ad16`, main after D-38. The configs and scripts added with this directory were untracked files on top of it.
- Stack: 8 partitions, one TaskManager with 4 slots, parallelism 4, one `*` subscription. The job and the oracle both ran at `--zone-offset +02:00` (D-3).
- Input: `data/daily/debs2022-gc-trading-day-08-11-21.csv`, MD5 `a08013f09a6b77355dae8b3ce141624d` as on Zenodo. It has 53,994,554 rows and 11,794,520 price events of 4,840 symbols.
- Reference: `oracle/out/monday`, the oracle on that file admitting every event: 373,126 results and 4,840 advisories. Monday needs a 25 ms bound in the oracle's source-order model, so no setting used here may reject an event. The admit-all output is therefore the reference of every run, and the job's `lateTicks` must be 0.
- Rewritten inputs: `correctness/run.sh inject` with `--seed 1` and `--disorder-ms 1000` or `300000` ([injection/](injection/)). Each price row moves by up to that much event time, and rows with the same symbol and event time keep their order (D-23).
- Harness: [bench/run.py](../../../bench/README.md) with the configs in [bench/configs/weekday/](../../../bench/configs/weekday/). Every harness run is drained, so its `lateTicks` is final. A live run starts the job, then replays paced from 09:00; the 745,472 rows before that go unpaced (745,627 of the file with 1 s of disorder).
- [bounded_defaults.py](bounded_defaults.py) runs the job with `--bounded` alone, which the harness cannot do: it states the bound and idleness in every run.

Each run is checked three ways:

- **The gate** of [the comparer](../../../correctness/README.md#compare): no EMA beyond 1e-9, no differing close, no missing, extra or duplicate result or advisory, a complete export, equal zone offsets, and a late count equal to the oracle's.
- **Counts.** The harness reconciles the rows replayed, read and classified, the price events, and the records written and exported, with each other and with the oracle's counts ([bench/README.md](../../../bench/README.md#counts-and-reconciliation)).
- **Digests.** [tables.py](tables.py) takes a SHA-256 of each run's results and of its advisories, over the D-18 fields in key order with every double as its eight bytes. Equal digests mean the same closes and EMAs bit for bit, which the gate's tolerance does not require.

[tables.py](tables.py) rebuilds [runs.csv](runs.csv) and [oracles.csv](oracles.csv) from the retained runs. `runs/<run ID>/` keeps each run's `config.json`, `summary.json` and comparison reports, and [oracle/](oracle/) each oracle run's manifest.

## The original file

| Replay | Bound | Idleness | `lateTicks` | Results | Advisories | EMA differences | Advisory differences | Gate | Published at end of input |
|---|---|---|---|---|---|---|---|---|---|
| preload | 64,265,001 ms | off | 0 | 373,126 | 4,840 | 0 | 0 | pass | 373,126 |
| preload, `--bounded` without watermark flags | default | default | 0 | 373,126 | 4,840 | 0 | 0 | pass | 373,126 |
| preload | 60 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 180 |
| preload | 1 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 97 |
| 1000× | 1 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 97 |
| 300× | 1 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 97 |
| 100× | 1 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 97 |
| 10× | 1 s | 30 s | 0 | 373,126 | 4,840 | 0 | 0 | pass | 7 |

- Every run published the oracle's 373,126 results and 4,840 advisories, rejected no event, and passed the gate.
- Every run has the oracle's two digests, `5797fba6d0fd…` for the results and `99e62d7c9c1b…` for the advisories. The job's closes and EMAs equal the oracle's bit for bit, at every setting and speed. That includes the 50 windows of Monday where binary64 is more than 1e-9 from the exact recurrence ([oracle/monday.json](oracle/monday.json), `precision`): both implementations round alike.
- Every harness run reconciled: 53,994,554 rows replayed and read; 11,794,520 price events, 42,137,822 rows without a price, 62,212 with `Last` = 0 and 10,873,721 dated from `Date`, all equal to the oracle's; 377,966 records written and exported.
- The first two rows are the defaults of a `--bounded` job (D-38). Their bound of 17.85 h is longer than the 15.5 h between Monday's first and last price event, so the watermark passes no window and all 373,126 are evaluated when the input ends. Only the other runs evaluate windows as the watermark passes them: all but the last 180 at 60 s, and all but the last 97 at 1 s. The preloads at 60 s and 1 s are those settings at the highest replay speed.
- At 10× only 7 results waited for the end of input. There the 30 s idleness timeout is 5 min of feed time. In the day's last minutes the partitions without a recent price event went idle and stopped holding the watermark back, which released the other 90 results 0.7 s before the replay ended. Nothing was late.
- 60 s and 30 s were the job's defaults when #21 was written. 1 s and 30 s are a live job's defaults now (D-38).
- The run without watermark flags is `runs/monday-bounded-defaults-af7ad16/`: the jar submitted with only `--bounded` to the stack of the first preload, writing to fresh output topics. A bounded job's counters vanish with it, so `lateTicks` comes from sampling the TaskManager until the job ended. The sum of each subtask's last sample has all 11,794,520 price events received and 0 late, so it is final. One scrape is not enough: the subtasks finish at different times, and in a first attempt the last scrape held three of the four.

| Live run | Rate achieved | Replay took | Job age when the replay started | Send lag p50 / p99 / max |
|---|---|---|---|---|
| 1000× | 944.5 | 53 s | 10.7 s | 10.5 / 19.5 / 19.6 s |
| 300× | 300.0 | 163 s | 10.9 s | 1 / 750 / 810 ms |
| 100× | 100.0 | 485 s | 10.7 s | 1 / 10 / 316 ms |
| 10× | 10.0 | 4,835 s | 11.2 s | 1 / 16 / 185 ms |

- Each replay started within the 30 s idleness timeout of its job, as D-38 requires for the unpaced rows before 09:00.
- At 1000× the replayer could not keep its schedule on the shared host and fell up to 19.6 s behind. That is still under the idleness timeout, and nothing was late.
- The 10× run took 80.6 min on a second stack, beside all the other runs.

## Reordered input

The oracle gives all three files the same results and advisories, bit for bit ([oracles.csv](oracles.csv)). What changes is the bound the file needs in the source-order model:

| Input | Price events late at a 0 ms bound | Bound needed |
|---|---|---|
| original | 79 | 25 ms |
| 1 s of disorder | 9,223 | 981 ms |
| 5 min of disorder | 5,884,256 | 298,801 ms |

An event is late at a 0 ms bound when an earlier row already has an event time at or past its window's end.

Each run is compared with the oracle run on its own file, which the harness requires, and with the oracle run on the original file.

| Input | Replay | Bound | Idleness | `lateTicks` | Results | Differences from the oracle on the same file | Differences from the oracle on the original | Gate, both | Published at end of input |
|---|---|---|---|---|---|---|---|---|---|
| 1 s of disorder | preload | 64,265,001 ms | off | 0 | 373,126 | 0 | 0 | pass | 373,126 |
| 1 s of disorder | preload | 60 s | 30 s | 0 | 373,126 | 0 | 0 | pass | 180 |
| 1 s of disorder | 100× | 1 s | 30 s | 0 | 373,126 | 0 | 0 | pass | 97 |
| 5 min of disorder | preload | 10 min | off | 0 | 373,126 | 0 | 0 | pass | 366 |

- All four runs have the digests of the runs on the original file. Reordering within the bound changed no close, EMA or advisory.
- The 100× run uses a live job's default bound. The file needs 981 ms of it, and nothing was late. The replay achieved 100.0× with a send lag of 12 ms at p99 and 462 ms at most, and started 10.4 s after the job.
- With 5 min of disorder a price row can arrive a whole window late, and half of them arrive after their window's end has passed in event time. At a 10 min bound up to three windows of a symbol are open at a time, and all but the last 366 results were published as the watermark passed.

## Control

Zero differences mean something only if the gate can find one on this day. So the first run's export was also compared with an oracle run at `--watermark-bound-ms 0`, which rejects 79 events ([control/](control/compare-report.json)). The gate fails as it should:

- It refuses the reference, because the job rejected 0 events and the oracle 79.
- It finds 2,932 differences in 35 symbols: 38 windows with another close and 2,894 later EMAs of those symbols, up to 1.04 apart. All are classed `lateRejection` by the oracle.
- All 4,840 advisories still agree: every symbol's first-window BUY survives a changed close.

## What these runs do not show

- **Query 2 beyond the first window.** All 4,840 advisories of Monday are the BUY that the zero start gives every symbol's first window (D-7, D-27). No SELL and no later crossover occurs on one day. The week has them (D-28, D-38), and the full-week gate is #9.
- **Late rejection.** No event of Monday is late at any bound used here, so no run exercised `lateTicks` above 0 or the `late-ticks` side output. D-38 has the week's burst, and #38 exports the late events.
- **1×.** A paced Monday takes 13.4 h at 1×. The slowest run here is 10×.
- **Other layouts.** One partition count and one parallelism, each run once.
- **Restart, subscriptions and the weekend files**, which #9 lists.

## Reproducing

```bash
D=debs2022-gc-trading-day-08-11-21.csv
oracle/run.sh --out /out/monday data/daily/$D
oracle/run.sh --out /out/monday-bound0 --watermark-bound-ms 0 data/daily/$D
for B in 1000:1s 300000:5min; do
  correctness/run.sh inject data/daily/$D --disorder-ms ${B%:*} --seed 1 --out /data/injected/monday-disorder-${B#*:}
  oracle/run.sh --out /out/monday-disorder-${B#*:} data/injected/monday-disorder-${B#*:}/$D
done

for config in bench/configs/weekday/*.json; do python3 bench/run.py "$config"; done
python3 docs/validation/2026-10-13-weekday/bounded_defaults.py --oracle oracle/out/monday --name monday-bounded-defaults
bench/.venv/bin/python docs/validation/2026-10-13-weekday/tables.py
```

`bounded_defaults.py` needs the stack that a preload of the original file has left up, which the last config in that order is. The 10× run takes 80 minutes; here it ran on a second stack with its own `COMPOSE_PROJECT_NAME`, `COMPOSE_NETWORK` and host ports ([README](../../../README.md#quickstart)).
