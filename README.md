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

The Flink job computes both queries for every symbol (`ticks` → `ema-results` and `advisories`), but publishes only symbols that some trader subscribes to. Write the subscriptions first: at startup the job holds its results back until it has read the `subscriptions` topic as it was at submission. This subscribes to every symbol:

```bash
echo 'all|{"trader":"all","symbols":["*"]}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic subscriptions --reader-property parse.key=true --reader-property key.separator='|'
```

Then submit the job. The jar is baked into the image at `/opt/job/flink-job.jar`. Rebuild with `docker compose up -d --build` after code changes.

```bash
docker compose exec jobmanager flink run -d -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar
```

For a finite replay, add `--bounded`. The job then reads the offsets of `ticks` and `subscriptions` that exist at startup, evaluates every window including the last, and finishes. Other options, all provisional until the dataset analysis: `--watermark-bound-ms` (default 60000), `--idleness-ms` (30000, 0 = off) and `--zone-offset` (`Z`). They are documented in `TradingTrendsJob`. The subscription format, how changes take effect, and how to rerun are in [docs/interfaces.md](docs/interfaces.md).

The job's counts are exported to Prometheus, and [docs/interfaces.md](docs/interfaces.md#counts-d-28) lists them. The parser's are under `flink_taskmanager_job_task_operator_deserializer_ticks_*`, the rest under `flink_taskmanager_job_task_operator_*`.

The producer uses the `tools` profile and is not built by the default start command. Build it explicitly:

```bash
docker compose build producer
```

The producer image contains the replayer in `/app/producer.jar`. Compose mounts `data/` read-only at `/data`, so put the day files there (see [Dataset](#dataset)). This preloads every day file in `data/` into `ticks`, in day order, and keeps the JSON report in the gitignored `bench/results/raw/`:

```bash
mkdir -p bench/results/raw
docker compose run --rm -T producer fi.aalto.cse4780.replay.Replayer --input /data > bench/results/raw/replay.json
```

The report says whether the replay completed and which offsets it wrote. Progress goes to stderr. `--mode realtime --rate 10` paces the rows at ten times real time instead, and `--exchanges`, `--symbols` and `--limit` select a subset. [docs/replayer.md](docs/replayer.md) covers the modes, the report, how to verify a replay against Kafka and the source files, and how to reset `ticks`.

The verification fixture below exercises the packaged Kafka connector independently of Query 1.

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

## Dataset profile

The dataset is CC BY-NC-SA 4.0 and is never committed. `analysis/` downloads a
reproducible sample of one weekday file and profiles it; the measured results and the
data contract they justify are in [docs/data-contract.md](docs/data-contract.md).

```bash
analysis/src/fetch_slice.sh     # ~1 GB stratified sample into data/slice (gitignored)
analysis/run.sh analysis/src/profile.py    # evidence tables into analysis/out/
analysis/run.sh analysis/src/golden.py     # golden fixtures into fixtures/golden/
```

`run.sh` uses the container in `analysis/Dockerfile`, which is what the report cites. Set
`USE_VENV=1` to run the same scripts in a local virtualenv instead, which is faster for
iteration because bind mounts on macOS are slow for multi-GB scans. Both paths produce
the same output.

The analysis container runs read-only, with `--network none`, `--cap-drop ALL` and
`no-new-privileges`. `data/` is mounted read-only and `analysis/out/` is the only
writable path. Only `fetch_slice.sh` touches the network, and it runs on the host.

### Container file access

Containers can read host files only where a bind mount grants it, and every mount in this
repo is a project-relative path. To enforce that rather than rely on convention, restrict
Docker Desktop's shared paths (Settings -> Resources -> File sharing) to this repository
alone. The defaults include `/Users`, so without that change a bind mount anywhere under
your home directory would be permitted.

## Dataset

Download the daily CSVs from Zenodo into `data/`, which is gitignored. Check the free disk space first.
