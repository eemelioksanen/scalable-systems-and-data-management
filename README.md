# Detecting Trading Trends in Financial Tick Data

CS-E4780 Scalable Systems and Data Management course project (Aalto, 2026). This is a Kafka + Apache Flink system that runs two queries over the [DEBS 2022 Grand Challenge trading data](https://doi.org/10.5281/zenodo.6382482):

- **Query 1:** EMA₃₈ and EMA₁₀₀ per symbol over 5-minute tumbling windows.
- **Query 2:** buy/sell advisories on EMA crossovers.

- Task: `CS_E4780_Course_Project_2026.pdf`
- Grading: `Evaluation_Criteria_26.pdf`
- Plan: [docs/ROADMAP.md](docs/ROADMAP.md)
- Decisions: [docs/decisions.md](docs/decisions.md)
- Interfaces (topic formats, oracle comparison): [docs/interfaces.md](docs/interfaces.md)

## Requirements

Docker only. No JDK or Maven is needed on the host: builds run inside containers, and `.devcontainer/` gives the IDE a JDK 17 + Maven environment.

## Quickstart

Start Kafka, create the topics, and start Flink, Prometheus and Grafana:

```bash
docker compose up -d --build
```

Scale out Flink from one TaskManager to two, giving four slots with the defaults:

```bash
docker compose up -d --scale taskmanager=2
```

Run the Maven tests. `scripts/mvn.sh` runs Maven in a container with a cached dependency volume, which is faster for day-to-day work than a full image build:

```bash
scripts/mvn.sh test
```

```bash
docker build --target test --no-cache-filter test .
```

The test target runs Maven `clean verify`, including the Query 1 unit and Flink mini-cluster tests. The runtime checks below exercise the infrastructure and packaged dependencies.

Stop and resume the same containers, retaining Kafka data:

```bash
docker compose stop
docker compose start
```

Reset the stack, removing the containers and their temporary data:

```bash
docker compose down --volumes
docker compose up -d --build
```

| UI | URL |
|---|---|
| Flink | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Kafka from the host | `localhost:29092` |

Kafka has no persistent data volume. `stop` retains data in its container; `down`, including without `--volumes`, removes that container and loses its topics, records, subscriptions, and consumer offsets. On the next start, `kafka-init` recreates the five empty application topics. `--volumes` also removes anonymous volumes used by the monitoring images.

Submit the Flink job (Query 1: `ticks` → `ema-results`). The jar is baked into the image at `/opt/job/flink-job.jar`. Rebuild with `docker compose up -d --build` after code changes.

```bash
docker compose exec jobmanager flink run -d -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar
```

For a finite replay, add `--bounded`. The job then reads the offsets that exist at startup, evaluates every window including the last, and finishes. Other options, all provisional until the dataset analysis: `--watermark-bound-ms` (default 60000), `--idleness-ms` (30000, 0 = off) and `--zone-offset` (`Z`). They are documented in `TradingTrendsJob`.

Parser counters (`priceEvents`, `nonPriceEvents`, `missingTimestampEvents`, `malformedEvents`) and `lateTicks` are exported to Prometheus under `flink_taskmanager_job_task_operator_*`.

The producer uses the `tools` profile and is not built by the default start command. Build it explicitly:

```bash
docker compose build producer
```

The producer image contains `/app/producer.jar`; its replayer entry point will arrive in a later task. The verification fixture below exercises the packaged Kafka connector independently of Query 1.

`TICKS_PARTITIONS` defaults to 8, `TM_SLOTS` to 2, and `PARALLELISM` to 2. Changing the partition setting does not resize existing topics; reset a disposable stack to apply it. More TaskManagers need more Docker memory; the two-TaskManager check was designed for an 8 GiB Docker VM.

Host ports can be changed with `KAFKA_HOST_PORT`, `FLINK_HOST_PORT`, `PROMETHEUS_HOST_PORT`, and `GRAFANA_HOST_PORT`. Kafka advertises the selected host port; clients inside Compose use `kafka:9092`. `docker compose -p <project>` also isolates the built image tags.

## Verify the local stack

The verification script builds both images, runs the containerized Maven target, tests both Kafka connection paths, executes a bounded Flink Kafka roundtrip, checks slots before and after scaling, and queries real Flink metrics through both Prometheus and Grafana. It also proves that stop/start retains Kafka records and reset removes them.

The checker needs Python 3.9+ and one Python client to reach the host Kafka listener. It uses its own Compose project, unused host ports, and synthetic topics, then removes its containers and images. Base images and build caches remain available for subsequent runs.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r scripts/validation/requirements.txt
.venv/bin/python scripts/verify-local-stack.py
```

Each run prints its evidence directory, containing `commands.log`, `results.json`, and metric samples. Use `--output <new-directory>` to choose the location. A failed check returns a nonzero exit code and retains its evidence. [Validation notes](docs/local-stack-validation.md) record the observed results and limitations.

## Dev container

Open the repository using your IDE's dev-container support, then run `mvn -B -ntp clean verify` in its terminal. JDK 17, Maven, Python, and the Docker CLI run inside the container. Run the Compose commands above from the host terminal, so its bind-mount paths refer to the host workspace.

To include the same workflow in automated validation, use the reference dev-container CLI. This optional setup needs Node.js and pnpm on the host:

```bash
validation_cli_dir=$(mktemp -d)
pnpm --dir "$validation_cli_dir" add @devcontainers/cli@0.89.0
.venv/bin/python scripts/verify-local-stack.py \
  --devcontainer "$validation_cli_dir/node_modules/.bin/devcontainer"
```

This builds `.devcontainer/devcontainer.json`, runs Maven `clean verify` inside it, and checks access to the host Docker daemon and Compose. The CLI step is opt-in; a run without it explicitly records that the dev container was not checked.

## Dataset

Download the daily CSVs from Zenodo into `data/`, which is gitignored. Check the free disk space first.
