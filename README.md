# Detecting Trading Trends in Financial Tick Data

CS-E4780 Scalable Systems and Data Management course project (Aalto, 2026). This is a Kafka + Apache Flink system that runs two queries over the [DEBS 2022 Grand Challenge trading data](https://doi.org/10.5281/zenodo.6382482):

- **Query 1:** EMA₃₈ and EMA₁₀₀ per symbol over 5-minute tumbling windows.
- **Query 2:** buy/sell advisories on EMA crossovers.

- Task: `CS_E4780_Course_Project_2026.pdf`
- Grading: `Evaluation_Criteria_26.pdf`
- Plan: [docs/ROADMAP.md](docs/ROADMAP.md)
- Decisions: [docs/decisions.md](docs/decisions.md)

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

## Dataset

Download the daily CSVs from Zenodo into `data/`, which is gitignored. Check the free disk space first.
