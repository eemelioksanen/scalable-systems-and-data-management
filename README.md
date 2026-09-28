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

Scale out Flink:

```bash
docker compose up -d --scale taskmanager=4
```

Run the Maven tests:

```bash
docker build --target test .
```

Stop everything (this also wipes the Kafka topic data):

```bash
docker compose down
```

| UI | URL |
|---|---|
| Flink | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Kafka from the host | `localhost:29092` |

Submit the Flink job. The jar is baked into the image at `/opt/job/flink-job.jar`:

```bash
docker compose exec jobmanager flink run -d -c <MainClass> /opt/job/flink-job.jar
```

Run the replayer. It reads the CSVs from `./data`, mounted at `/data`:

```bash
docker compose run --rm producer <MainClass> [args]
```

These environment variables tune the stack: `TICKS_PARTITIONS` (default 8), `TM_SLOTS` (2) and `PARALLELISM` (2).

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
