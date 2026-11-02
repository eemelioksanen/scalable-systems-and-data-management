# Late-event export verification

Revision `35eb8f69e9d95775256a994469bb333f36e61423` passed the live late-event check and a bounded admit-all check on 1 November 2026. [verification.json](verification.json) records the outcomes. This is synthetic correctness evidence, not a performance measurement or a full-week correctness gate.

## Environment and scope

Podman 6.1.4 on macOS, with an arm64 Linux VM, 7 CPUs and 8 GiB RAM. Docker Compose 5.6.1 ran through `podman compose`. The isolated project was `issue38`, with Kafka on host port 29138 and Flink on 8138. It used one Kafka partition, one TaskManager with two slots, parallelism 1 and an all-symbol subscription. Both job and oracle used `+02:00`.

`podman run --rm -v "$PWD":/src -v trading-trends-m2:/root/.m2 -w /src docker.io/library/maven:3.9-eclipse-temurin-17 mvn -B -ntp clean verify` passed 168 Java tests: 37 producer, 93 Flink and 38 dashboard backend. The 39 Python correctness tests also passed using Python 3.14.10 and Polars 1.44.2.

The running Flink containers used `flink:2.2.1-java17` with the newly packaged `flink-job/target/flink-job.jar` mounted at `/opt/job/flink-job.jar`. The producer ran the newly packaged `producer/target/producer.jar` in `eclipse-temurin:17-jre`. Kafka used the project's existing image with Kafka 4.3.1. The verification did not rebuild the complete multi-stage Dockerfile or run the dashboard frontend tests.

## Fixture and results

The fixture has 100 price events for `SYNTH.NL`, one minute apart from 09:00 on 8 November 2021. Prices are `100 + n % 17`. Each price event is preceded by 256 quote-only rows at its system time, giving 25,700 rows. These quote rows advance the replayer's 256-row smoothing clock without affecting the calculation.

The first attempt used only the 100 price rows. Its clock did not advance, so it sent the file unpaced and rejected no events. That attempt does not count as late-event evidence.

The existing injector rewrote the larger fixture with `--late-rate 0.15 --late-by-ms 600000 --seed 7`. [injection.json](injection.json) retains the input/output checksums and the expected late count of 13. The identities are retained in [late-events.csv](late-events.csv).

The live job used a 1,000 ms watermark bound, idleness off and 10-second checkpoints. After all tasks reached RUNNING, the replayer ran at 120 times source time. [replay.json](replay.json) records the actual pacing and acknowledged offsets. Once the source had read all 25,700 records, `flink stop --drain` completed the job.

- All 13 exported `(sourceFile, sourceLine, symbol)` identities equal the injector's identities, including multiplicity. [late-events.csv](late-events.csv) joins them with the exported window ends and injector bounds.
- The oracle ran on the rewritten file with the exported `late_events.parquet` as `--exclude-events`. The comparison passed with 20 EMA results, one advisory, zero differences and no duplicates. See [live-comparison.json](live-comparison.json), [live-oracle.json](live-oracle.json) and [live-export.json](live-export.json).
- The actual q1 vertex wrote 33 records, equal to 20 EMA results plus 13 late events. The benchmark's `Run.reconcile` passed using the real replay report, export and Flink vertex metrics. See [live-reconciliation.json](live-reconciliation.json).

The output topics were then deleted and recreated inside the isolated project, leaving the input and subscription topics in place. A new `--bounded` job used its default 64,265,001 ms bound and disabled idleness. It admitted every price event and matched an admit-all oracle. See [bounded-comparison.json](bounded-comparison.json), [bounded-oracle.json](bounded-oracle.json) and [bounded-export.json](bounded-export.json).

`Run.reconcile` and `Run.summarize` consumed that bounded job's actual replay report, Flink vertex metrics, export and comparison. Every sampled counter was deliberately unavailable. The resulting [bounded-summary.json](bounded-summary.json) has `lateTicks: 0`, `countersFinal.lateTicks: true`, successful reconciliation and correctness verdict `pass`. The source split remains non-final, as expected without samples. This checks the changed benchmark logic against real output; it does not claim a complete `bench/run.py` run with Prometheus and cAdvisor.

## Reproduce the fixture

From the repository root, generate the source file:

```python
from datetime import datetime, timedelta
from pathlib import Path

out = Path("data/late-export/source")
out.mkdir(parents=True, exist_ok=True)
lines = []
for n in range(100):
    time = datetime(2021, 11, 8, 9) + timedelta(minutes=n)
    fields = [""] * 40
    fields[:4] = ["SYNTH.NL", "E", "08-11-2021", time.strftime("%H:%M:%S.000")]
    lines.extend([",".join(fields)] * 256)
    fields[21] = str(100 + n % 17)
    fields[23] = time.strftime("%H:%M:%S.000")
    fields[26] = "08-11-2021"
    lines.append(",".join(fields))
(out / "synthetic-08-11-21.csv").write_text("\n".join(lines) + "\n")
```

Inject using the repository's correctness environment:

```bash
PYTHONPATH=correctness:oracle correctness/.venv/bin/python -m ema_check inject \
  data/late-export/source/synthetic-08-11-21.csv \
  --late-rate 0.15 --late-by-ms 600000 --seed 7 --out data/late-export/injected
```

Start a fresh, isolated stack from the repository Compose file with one partition and parallelism 1. Build the images, or mount the jars produced by `clean verify` as above. Publish the README's all-symbol subscription. Submit the live job with:

```bash
flink run -d -p 1 -Dexecution.checkpointing.interval=10s \
  -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar \
  --watermark-bound-ms 1000 --idleness-ms 0
```

Wait for every job vertex to reach RUNNING. Run `fi.aalto.cse4780.replay.Replayer --input /data/late-export/injected --mode realtime --rate 120` in the producer container and save its JSON report. Wait until the source's `read-records` reaches 25,700, then use `flink stop --drain --savepointPath file:///tmp/late-export-savepoints <job-id>`. Wait for FINISHED before exporting.

Export with the matching `--replay-report`, run the oracle with the export as `--exclude-events`, and compare with `--job-late-ticks 13 --job-late-events <export>/late_events.parquet`. Check equality of the exported and injected source identities as well. For the bounded check, recreate only the three output topics, submit `--bounded`, and compare against an oracle without exclusions using late count zero.

The verification stack was removed after collecting the evidence. The reusable Maven dependency cache and existing images were retained.
