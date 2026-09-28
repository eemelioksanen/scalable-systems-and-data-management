# Replayer

`producer/` replays DEBS 2022 day files into the Kafka `ticks` topic, one record per CSV data row.
The record format is in [interfaces.md](interfaces.md#ticks-d-13-d-14-d-15-d-29), and the design
decisions are D-13 to D-15, D-23, D-24 and D-29 in [decisions.md](decisions.md). This page covers
how to run it, what a finished replay reports, and the measurements behind its defaults.

## Running it

The replayer runs in the `producer` image (`docker compose build producer`), which Compose starts with
`data/` mounted read-only at `/data`. Use `run -T`, so the JSON report on stdout stays separate from
the progress log on stderr.

```bash
# Preload the whole week (every *.csv in data/, in day order) and keep the report.
docker compose run --rm -T producer fi.aalto.cse4780.replay.Replayer --input /data > bench/results/raw/preload.json

# Live replay at ten times real time, starting the clock at the open; rows before 09:00 go unpaced.
docker compose run --rm -T producer fi.aalto.cse4780.replay.Replayer --input /data \
  --mode realtime --rate 10 --pace-from 2021-11-08T09:00 > bench/results/raw/live-10x.json

# One exchange, as fast as possible, while the Flink job runs.
docker compose run --rm -T producer fi.aalto.cse4780.replay.Replayer --input /data --mode max --exchanges NL

# Read and project without Kafka: the replayer's own ceiling, and the partition load for 16 partitions.
docker compose run --rm -T --no-deps producer fi.aalto.cse4780.replay.Replayer --input /data --dry-run --partitions 16
```

`--help` lists every option. The main ones:

| Option | Meaning |
|---|---|
| `--input PATH` | A CSV file, or a directory whose `*.csv` files replay in the day order of their DEBS names (`…-08-11-21.csv`). Repeatable; files named explicitly keep their order. |
| `--mode preload\|max\|realtime` | See [Modes](#modes). Default `preload`. |
| `--rate K`, `--pace-from T` | Real-time only: K times faster than real time; rows before source time T are sent unpaced. |
| `--exchanges FR,NL`, `--symbols …`, `--symbols-file F` | Replay only these exchanges (the `ID` suffix) or symbols. Both together means both must match. |
| `--limit N` | Stop after N records. |
| `--topic`, `--bootstrap-servers`, `--producer-property K=V` | Kafka target and producer overrides. The bootstrap default is `$BOOTSTRAP_SERVERS` (`kafka:9092` in Compose). |
| `--dry-run [--partitions N]` | Read, project and assign partitions with Kafka's rule, without Kafka. |
| `--report FILE`, `--run-id ID`, `--checksum` | Also write the report to a file; name the run; record each input's MD5. |

The exit status is 0 only when the replay completed. SIGINT or SIGTERM (Ctrl-C, `docker stop`) stops
reading, flushes what was already sent, and reports `INTERRUPTED`.

## What it sends

Every data row becomes one record, including quote and housekeeping rows: there is no early price
filtering, so raw-event throughput stays comparable with the Flink job's input (an early-filtering
variant is #23). Comment lines and the header are skipped. A row that is neither 39 fields nor 40 with
an empty last one cannot be projected, so it is counted as `rejected` and not sent. The 08.11.2021 file
has none.

- **Key:** the symbol (`ID`), so Kafka's default partitioner puts each symbol in one partition.
- **Value:** the 7-field projection, copied character for character. Prices and times are never
  parsed, so no precision is lost (D-24).
- **Header `src`:** `<file index>:<line number>`, the row's position in its source file.
- **Timestamp:** the wall-clock time the row was handed to the producer: the ingest time for latency.
  Replay speed changes only this; the value, and so every calculation timestamp, is the same.
- **Order:** one thread reads the files in order and sends every row. The producer is idempotent with
  `acks=all`, so each partition, and therefore each symbol, keeps file order through retries (D-23).

Memory stays bounded. Files are read line by line through a 1 MiB buffer, and the producer's
32 MiB buffer blocks the reader when Kafka falls behind. A full weekday preload ran with a 256 MiB
heap (see [Validation](#validation)).

## Modes

| Mode | Pacing | Producer batching | Use |
|---|---|---|---|
| `preload` | none | `linger.ms=50`, `batch.size=1 MiB` | Load the input before the job starts, for throughput runs that must not be limited by the replayer. |
| `max` | none | Kafka defaults (`linger.ms=5`, 16 KiB) | Push as fast as possible while the job runs. |
| `realtime` | source clock ÷ `--rate` | Kafka defaults | Latency runs at 1×, 10×, 100× real time. |

All modes use lz4 compression (Kafka's default lz4 level, 9), `acks=all` and idempotence. `--producer-property` overrides any
producer setting, and the report records the ones the replayer set.

### The source clock

Live replay paces rows by **when the feed delivered them**, estimated from the system `Date` and
`Time` columns ("system time for last received update"). It does not use `Trading time`, which is
empty on non-price rows.

The raw `Time` cannot pace a replay by itself. Measured on the whole 08.11.2021 file:

- The file opens with a snapshot of about 100,000 rows whose times span the whole day. They include
  the previous close (a block of 17:35–17:36 rows around line 30,800) and midnight. A running maximum
  of `Time` reaches 18:30:34 at line 195, which would send the rest of the day unpaced. A running
  median over 255 rows reaches 17:35 by line 30,859.
- From about line 100,000 to line 53,053,772 the column is ordered to within a second: no row is more
  than 60 s behind the running maximum.
- From line 53,053,773, at the close, a second dump holds 328,165 rows more than 60 s behind.
- 11,999 rows have no `Date` or `Time` at all.

So the clock is the **minimum `Date`+`Time` over the last 256 rows that have one, never moving
backward** (`SourceClock`). Rows stamped ahead of their neighbours cannot pull it forward, and stale
rows only hold it. On the 08.11.2021 file, 99.06% of the rows with a time are within 1 s of the clock.
The opening snapshot is paced over 01:01–07:15. The clock reaches 09:01:00 at row 1,000,000 and
17:30:27 at row 53,000,000, and ends at 22:25:15. The clock follows every row, including rows outside
a `--symbols` or `--exchanges` subset, so a subset replays at the feed's pace.

Real time includes the quiet hours. At 1× the snapshot waits about six hours for the first real
updates, and a multi-day replay waits out each night and the weekend. `--pace-from` sends everything
before a source time at full speed, waits until Kafka has acknowledged it, and starts the clock there.
Without that wait, the unpaced backlog still queued in the producer delayed the first paced rows
(during development, p99 lag at 100× fell from 139 ms to 7 ms with it).

The feed arrives in once-a-second bursts. From 09:00 to 09:16, 99.9% of rows have a whole-second
`Time`, in groups of up to 8,709 rows with the same value. A faithful replay therefore sends each second's
rows together, and the pacing lag is mostly the time it takes to push a burst out. It is higher at
lower rates, where the CPU clocks down between bursts: on this machine (powersave governor) p50 is 2 ms
at 100× and 4 ms at 10×.

## The replay report

The report is the replay's completion record. A consumer treats a replay as a finite input only when
`status` is `COMPLETE`.

```json
{
  "runId" : "preload-weekday", "status" : "COMPLETE", "mode" : "preload", "topic" : "ticks", "partitions" : 8,
  "files" : [ { "index" : 0, "path" : "/data/debs2022-gc-trading-day-08-11-21.csv", "bytes" : 4672671094,
                "lines" : 53994567, "complete" : true, "md5" : "a08013f09a6b77355dae8b3ce141624d" } ],
  "counts" : { "lines" : 53994567, "comments" : 12, "headers" : 1, "rejected" : 0, "dataRows" : 53994554,
               "excluded" : 0, "attempted" : 53994554, "acknowledged" : 53994554, "failed" : 0, … },
  "offsets" : [ { "partition" : 0, "startOffset" : 0, "endOffset" : 7033232, "records" : 7033232 }, … ],
  "timing" : { "durationMs" : …, "recordsPerSecond" : …, … },
  "producerConfig" : { … }
}
```

- **Accounting:** `lines = comments + headers + rejected + dataRows`, `dataRows = excluded + attempted`,
  and a completed replay has `attempted = acknowledged` and `failed = 0`.
- **Offsets:** each partition's `[startOffset, endOffset)` holds exactly this run's records when
  `endOffset - startOffset = records`, that is, when nothing else wrote to the topic meanwhile. A
  consumer reads those ranges, or, after a `COMPLETE` preload into an otherwise idle topic, simply up to
  the current end offsets, which is what the Flink job's `--bounded` does.
- **`FAILED`:** the first record Kafka does not acknowledge stops the replay. Sending more would leave a
  gap no consumer could see. The topic then holds a partial input: reset it before retrying. An input
  that cannot be read fails the same way.
- **`INTERRUPTED`:** stopped by a signal. Everything attempted was flushed and is accounted for.
- **`pacing`** (real-time only): the source clock at the start, where pacing began and at the end; the
  rate achieved; and p50/p99/max of how late rows were sent against their schedule. A growing lag means
  the replayer, not the system under test, limits the rate.

## Verifying a replay

`ReplayVerifier` reads a report's offset ranges back from Kafka with a plain consumer, and with
`--source` re-reads the input files independently of the replayer's code:

```bash
docker compose run --rm -T producer fi.aalto.cse4780.replay.ReplayVerifier --report - --source \
  < bench/results/raw/preload.json
```

The Kafka side checks that each range holds the reported count with no offset gaps. It also checks that
every record sits in the partition Kafka's default partitioner gives its key and that each symbol uses
one partition. Each value must have 7 fields led by its key. Each symbol's `src` positions must
increase, and every timestamp must be a create time within the run. The source side finds the 7
columns by header name, splits rows naively, applies the report's selection, and computes a SHA-256
per partition over key, value and `src`. Equal digests on both sides show every field and each
partition's order were preserved. `contentDigest` leaves out offsets and timestamps, so replays of the
same rows at different speeds must produce the same one.

## Resetting and replaying again

The replayer appends. The Flink job reads `ticks` from the earliest offset, so a second replay into the
same topic doubles its input. Reset the topic first. Deleting it and rerunning `kafka-init` recreates
it empty with the configured partitions:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic ticks
docker compose up kafka-init
```

`docker compose down` also resets it, along with every other topic. Replaying the same input again
produces the same records apart from their timestamps and, if another replay wrote to the topic, their
offsets.

## Partitions

`ticks` has 8 partitions locally (`TICKS_PARTITIONS`), keyed by symbol with Kafka's murmur2 rule. On the
08.11.2021 file the 8 partitions receive 5.44 M to 8.58 M rows, 1.27 times the mean at the top. That is
a raw-row count; the Flink job's load follows price events, which the dataset analysis (#4) measures.
`--dry-run --partitions N` shows the assignment for another partition count without a Kafka.

## Disk

Measured on this machine's broker, with the preload settings:

| Compression | Log bytes per record | Preload rate | 10 M rows |
|---|---|---|---|
| none | 73.7 | 1.29 M rec/s | 737 MB |
| lz4 (default) | 17.2 | 1.25 M rec/s | 172 MB |
| zstd | 10.9 | 0.92 M rec/s | 109 MB |

These are the first 10 M rows of 08.11.2021. The whole weekday took 4.07 GB uncompressed
(75.3 bytes per record), close to the 4.67 GB CSV, and 0.96 GB with lz4 (17.7 bytes per record).
Kafka also preallocates 20 MiB of index files per partition. lz4 level 1, Kafka's fast codec, stored
16.4 bytes per record but preloaded at only 1.12 M rec/s, so the default level stays.

**Full week.** The week has 289,116,808 data rows (the oracle's full-week run, #5). At the weekday's lz4 rate that is about 5.1 GB
of log, against about 22 GB uncompressed. The input CSVs take another 25 GB if they sit
on the same disk. Before a full-week preload, check the free space where Docker keeps its data, which
is where Kafka's log lives:

```bash
df -h "$(docker info --format '{{.DockerRootDir}}')"
```

`ticks` is created with `retention.ms=-1`, so a preloaded week is not deleted after Kafka's default
7 days. Topics created before this setting keep the default: reset them, or
`kafka-configs.sh --alter --add-config retention.ms=-1`.

## Throughput

On an 11th-gen Intel i7-11700K (16 threads, 62 GiB, NVMe) with one Kafka broker in Docker:

| Run | Records/s |
|---|---|
| Dry run of the weekday: read, project, MD5, no Kafka | 2.22 M |
| Preload of the weekday, lz4 | 1.24 M |
| Preload of the weekday, lz4, 256 MiB heap | 1.13 M |
| Preload of the weekday, no compression | 1.35 M |
| `max`, first 3 M rows | 1.14 M |

These are the replayer's limits on this machine. A Flink job consuming a live replay at close to these
rates is measuring the replayer. Throughput runs therefore preload first.

## Validation

Run on 28 September 2026 for #6 against the stack in `docker-compose.yml`, from the image the
Dockerfile builds, on the input `debs2022-gc-trading-day-08-11-21.csv`, with nothing else running on
the host.
The reports, verifications and commands are in [validation/replayer-2026-09-28](validation/replayer-2026-09-28/).

| Check | Run | Result |
|---|---|---|
| Input structure | Dry run with `--checksum` | 53,994,567 lines: 12 comments, 1 header, 0 rejected, 53,994,554 data rows. The MD5 matches Zenodo's. |
| Bounded memory | Weekday preload with `JAVA_TOOL_OPTIONS=-Xmx256m` | `COMPLETE`: 53,994,554 records attempted and acknowledged in 47.8 s. |
| Ingestion, per a real consumer | `ReplayVerifier --source` on that preload | Passed. Kafka holds exactly the 53,994,554 records in the reported ranges, with no offset gaps. There are 5,493 symbols, each in its murmur2 partition only. Each symbol's `src` positions increase. Every timestamp is a create time within the run. Every partition's digest equals the independent re-read's. |
| Field preservation, compression | The same weekday without compression | Same `contentDigest` as the lz4 preload (`07e90659…`). |
| Identical content across speeds | First 3 M rows (to 09:16:42) in `preload`, `max`, and real time at 100×, 10× and 1× from 09:00 | All five verified, with one `contentDigest` (`6bf54071…`). |
| Rate control | The three real-time runs, 2,254,528 paced rows each | Achieved rate and lag p50/p99/max: 100×: 99.98, 2/10/15 ms. 10×: 10.00, 4/21/28 ms. 1×: 1.00, 4/21/30 ms. The lag is the per-second bursts described in [The source clock](#the-source-clock). |
| Signal | `docker stop` one minute into a 1× run | Exit 143 and `INTERRUPTED`: 1,000,680 records attempted, all acknowledged. The verifier rejects the report, but Kafka holds exactly those records and every record check passes. |
| Broker failure | `docker compose stop kafka` 3 s into a weekday preload, with a 15 s delivery timeout | Exit 1 and `FAILED` 19 s later, naming the expired batch: 4,605,648 attempted = 4,493,486 acknowledged + 112,162 failed. Kafka holds exactly the acknowledged records, every record check passes, and the verifier rejects the report. |
| Reset | Delete `ticks`, then `docker compose up kafka-init` | 53,994,554 records before; recreated empty with 8 partitions and `retention.ms=-1`. |

The unit tests (`scripts/mvn.sh -pl producer test`) cover the CSV structure, the source clock, pacing,
selection, limits, failure and interruption, and check that the verifier's re-read and the replayer
agree.
