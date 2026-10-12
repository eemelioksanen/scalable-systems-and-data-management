# Local stack validation

Validated on 28 September 2026 for [issue #1](https://github.com/eemelioksanen/scalable-systems-and-data-management/issues/1), using synthetic records on an Apple Silicon Mac.
Docker Desktop 4.93.0 provided Engine 29.8.1, Compose 5.5.1, and an 8 GiB Linux VM.
No host JDK or Maven was used.

The [captured result](validation/2026-09-28.json) records the clean project's identity, ports, checks, and source worktree state.
The [command excerpts](validation/2026-09-28-commands.txt) and [metric samples](validation/2026-09-28-metrics.json) retain portable evidence from the run.
After following the [README setup](../README.md#verify-the-local-stack), run:

```bash
.venv/bin/python scripts/verify-local-stack.py \
  --output /tmp/trading-trends-verification \
  --devcontainer "$validation_cli_dir/node_modules/.bin/devcontainer"
```

Choose a new output directory for each run.
The full logs stay in that local directory; the repository retains the result, selected command output, and metric samples.

## Observations

| Check | Command or observation | Result |
|---|---|---|
| Container build | `docker build --target test --no-cache-filter test` and `docker compose build jobmanager producer` | Maven `clean verify` passed for both modules, including 47 Query 1 tests; both runtime images built. |
| Producer dependency compatibility | Run `LoggingCheck.java` against the packaged producer jar inside the test image | `org.slf4j.simple.SimpleLoggerFactory`; a real INFO message appeared. |
| Kafka topics | Describe topics after `kafka-init` exited with code 0 | `ticks`, `ema-results`, and `advisories` each had 8 partitions; `stats` and `subscriptions` each had 1. `subscriptions` used `cleanup.policy=compact`. |
| Host listener | Python producer and consumer used `localhost:57248` | Broker metadata advertised the same address; the synthetic CSV record roundtripped. |
| Container listener | Kafka's console producer and consumer used `kafka:9092` | The container consumer read the host record; the host consumer read the additional container record. |
| Flink connector | SQL client loaded `/opt/job/flink-job.jar` and ran [the bounded fixture](../scripts/validation/kafka-roundtrip.sql) | Both records arrived unchanged in the output topic. One job finished, with no failed jobs. |
| Initial capacity | Flink `/overview` and `/taskmanagers` | 1 registered TaskManager, 2 total slots. |
| Scale | `docker compose up -d --no-build --scale taskmanager=2` | 2 registered TaskManagers, 4 total slots. |
| Metrics | Prometheus `/api/v1/targets` and instant queries, also through Grafana's datasource proxy | Healthy targets increased from 2 to 3 after scaling. Both APIs returned a registered-TaskManager gauge of 2 and two positive JVM heap samples. |
| Stop/start | `docker compose stop`, then `docker compose start --wait` | Both Kafka records remained readable. |
| Reset | `docker compose down --volumes`, then a clean start and `kafka-init` | Synthetic topics disappeared; only the five recreated application topics remained. |
| Default startup | `docker compose up -d --build`, with no overrides | Project `trading-trends` started on the default ports; Kafka roundtripped a synthetic record and Flink registered 1 TaskManager with 2 slots. |
| Plain down | `docker compose down`, then `docker compose up -d` | The synthetic topic and its record disappeared; only the five recreated application topics remained. |
| Dev container | Reference CLI `up`, then `exec` for `mvn -B -ntp clean verify`, `docker version`, and `docker compose version` | All commands passed inside the configured container, including the same 47 tests. |

Prometheus ran version 3.11.2 and the `grafana/grafana:12.2` tag resolved to Grafana 12.2.10.
Flink reported 2.2.1, and Kafka ran the committed 4.3.1 image.
The Flink Kafka connector remained at `5.0.0-2.2`, which the [Apache compatibility list](https://flink.apache.org/downloads/#apache-flink-kafka-connector-500) supports on Flink 2.2.x.

## Corrections

The original producer jar paired SLF4J API 1.7.36 with the pinned 2.0.20 provider.
Running the packaged jar returned `NOPLoggerFactory` and disabled logging.
Managing `slf4j-api` at the existing `${slf4j.version}` fixed the mismatch; the same runtime check then passed.

The dev-container Docker feature tried to install Moby packages on Debian trixie and explicitly rejected that combination.
Setting `moby` to `false` selected Docker CE and allowed the build to finish.
The generated feature lock records the exact feature versions used.
The accepted Flink, Kafka, Java, and Maven build choices remain unchanged.

## Boundaries

The Maven build runs the Query 1 unit and Flink mini-cluster tests added on `main` during this work.
The SQL fixture verifies the connector on the running cluster; it does not validate Query 1's numerical output or the replayer, subscriptions, or oracle.
The dev-container check uses the reference CLI and does not exercise an IDE's graphical integration.

This run verified the initial Flink scrape only.
D-35 added the Kafka and cAdvisor scrapes. The script now checks them, and D-35 records them under load.
Performance measurements and cloud deployment belong to later tasks.
Kafka remains temporary: removing its container loses all its records and offsets.
