# Correctness tools

The tools the correctness checks run (D-18, #9, #21):

- `export` reads `ema-results`, `advisories` and `late-ticks` from Kafka into Parquet with the oracle's field names.
- `latency` splits a live run's latency into its stages from an export made with `--ticks-topic ticks` ([bench/README.md](../bench/README.md#latency)).
- `compare` diffs an export against an [oracle](../oracle/README.md) run and classifies every difference.
- `inject` rewrites day files with bounded disorder and late events, deterministically from a seed.

They share no code with the Flink job or the oracle. The tests use the oracle as the judge of the injector's output.

## Running

Only Docker is needed. `USE_VENV=1` runs the same code in a local venv.

```bash
correctness/run.sh test
correctness/run.sh export --zone-offset +02:00 --out /out/run-1
correctness/run.sh compare --oracle oracle/out --job correctness/out/run-1 --job-late-ticks 0 --out /out/run-1-vs-oracle
correctness/run.sh inject data/daily/*.csv --disorder-ms 1000 --seed 1 --out /data/injected/disorder-1s
```

Results go to `correctness/out/` (gitignored), which the container sees as `/out`. `inject` writes to `data/injected/`, where the replayer can read it as `/data/injected/...`. `export` joins the Compose network (`COMPOSE_NETWORK`, default `trading-trends_default`) and reads `kafka:9092`.

## Export

Each topic is read from its start up to the end offsets taken when the export starts. `manifest.json` records both for every partition, with the record count. A topic is `complete` only if every partition starts at offset 0 and holds one record per offset up to its end. `compare` refuses an incomplete export.

Every exported row keeps its record's `kafkaTs` and `kafkaTsType`. `--ticks-topic ticks` also writes `price_events.parquet`, the partition, offset, record timestamp and event time of every record the job would parse as a price event.

Pass the job's `--zone-offset`: the export records it, uses it for price events' event times, and `compare` requires it to equal the oracle's (D-3).

Export only after the job has finished. A topic still being written to exports whatever it holds at that moment.

## Compare

The EMA check joins on `(symbol, windowEnd)`. `secType` and `close` must be equal, and both EMAs within `--tolerance` (1e-9, D-18). Advisories are compared as `(symbol, windowEnd, type)` identities, with precision and recall. Duplicates are found on each side before anything is joined.

Every difference gets a `kind` and a `difference` class:

| kind | Meaning |
|---|---|
| `oracleOnly`, `jobOnly` | The window or advisory exists on one side only. |
| `secType`, `close` | The value differs. |
| `ema` | Same close, an EMA outside the tolerance. |

| difference | Meaning |
|---|---|
| `lateRejection` | One side rejected an event of this symbol in this window or an earlier one, which changes every later EMA of the symbol. `rejectedBy` says which: the job (`--job-late-events`) or the oracle (its `rejected_events.parquet`). |
| `precisionDrift` | An `ema` difference of at most `--drift-ulps` (66) units in the last place. Binary64 drift alone exceeds 1e-9 on 1,270 windows of the week, up to 33 ulps from exact (D-27), so two correct implementations can be this far apart. |
| `pipelineLoss` | The oracle has the window, the job does not, and no rejection explains it. |
| `unexplained` | Anything else. Either side may be wrong: check the oracle's `closeSourceFile` and `closeSourceLine` against the input. |

The gate passes only if all of these hold:

- there are no differences or duplicates of any class
- the export is complete
- both sides used the same zone offset
- `--job-late-ticks` equals the oracle's `excluded` + `lateRejected`

A run whose late count differs from the oracle's is compared against the wrong reference. The oracle README says [which reference to use](../oracle/README.md#late-events-matching-a-batch-run-to-a-streaming-run).

The completed `late-ticks` export gives the late count even after a bounded job's metrics disappear. Pass `--replay-report <replay.json>` to `export` to resolve source filenames. With no late records, the empty export proves zero without a replay report. Missing source positions, unresolved file indices and duplicate source positions make the export incomplete.

`--job-late-events` takes the exported `late_events.parquet`, with `sourceFile`, `sourceLine`, `symbol` and `windowEnd`. The comparer checks its length against `--job-late-ticks` and its identities against the oracle's `rejected_events.parquet`. Equal counts alone cannot prove that the same events were rejected. Run the oracle on the replay's input files with `--exclude-events correctness/out/run-1/late_events.parquet`, then compare against that reference. The benchmark harness supplies the export and its count automatically.

Outputs: `report.json` (also printed), `ema_differences.parquet`, `advisory_differences.parquet`, `ema_duplicates.parquet` and `advisory_duplicates.parquet`. The exit code is 0 only if the gate passes.

## Inject

Bounded disorder (`--disorder-ms B`): let M be the largest event time among the price rows read so far. Each price row gets the sort key M + d, with d drawn from [0, B], and every other row gets M. Rows are written in key order. A price row with the same symbol and event time as an earlier one never gets a smaller key, so equal-timestamp events keep their order (D-23) and every window keeps its close. A row written before row i then has an event time of at most M_i + B. In the oracle's admission model, each event therefore needs at most B more bound than in the original file.

So a job run with a bound of at least the original's need plus B, and idleness off, must give exactly the original's oracle output. On Monday's full file, 1 s of disorder with `--seed 1` raised the needed bound from 25 ms to 981 ms, and the oracle's 373,126 windows and 4,840 advisories were unchanged. So were the job's, in a preload and live at 100× ([evidence](../docs/validation/2026-10-13-weekday/README.md#reordered-input)).

Late events (`--late-rate p --late-by-ms L`): each price row is chosen with probability p and held back until the rows already written reach its window end plus L. `late_events.parquet` lists each one as `(sourceFile, sourceLine, symbol, minBoundMs)`, which is the oracle's admission model applied to the rewritten file. A held row still pending at the end of its file is written there, and is listed only if that leaves it late. Compare a run on these files against an oracle run on the same files.

Event times follow the oracle's default missing-date policy (D-25), and the bound claim holds only under it. Windows are taken in the files' local time, which matches any zone offset that is a multiple of 5 minutes, such as D-3's `+02:00`. The output directory must be empty, since the replayer reads every `.csv` in it. The rewritten files keep their names, so the oracle and the replayer order them as before, and rows never move between files. `injection.json` records the options and the MD5 of every input and output. One day file takes about 85 s and 350 MB.
