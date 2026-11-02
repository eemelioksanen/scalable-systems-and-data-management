#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
evidence="$PWD/docs/validation/2026-11-01-latency"
scratch="$(mktemp -d)"
mkdir -p "$scratch/bin" "$scratch/images/flink-job/target" "$scratch/images/producer/target"
cat > "$scratch/bin/docker" <<'PYTHON'
#!/opt/homebrew/bin/python3.14
import os, sys
args = sys.argv[1:]
if args and args[0] == 'run' and 'trading-trends/correctness:dev' in args and '--user' in args:
    args[args.index('--user') + 1] = '0:0'
os.execv('/opt/homebrew/bin/podman', ['podman', *args])
PYTHON
chmod +x "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH"
export COMPOSE_PROJECT_NAME=issue37-latency
export COMPOSE_FILE="$PWD/docker-compose.yml:$evidence/podman.override.yml"
export COMPOSE_NETWORK=issue37-latency_default
export FLINK_HOST_PORT=18081 PROMETHEUS_HOST_PORT=19090 KAFKA_HOST_PORT=39092 VIZ_HOST_PORT=18088 GRAFANA_HOST_PORT=13000
podman run --rm -v "$PWD":/src -v trading-trends-m2:/root/.m2 -w /src \
  docker.io/library/maven:3.9-eclipse-temurin-17 mvn -B -ntp clean verify
cp flink-job/target/flink-job.jar "$scratch/images/flink-job/target/"
cp producer/target/producer.jar "$scratch/images/producer/target/"
podman build -f "$evidence/flink.Dockerfile" -t issue37-latency/flink-job:dev "$scratch/images"
podman build -f "$evidence/producer.Dockerfile" -t issue37-latency/producer:dev "$scratch/images"
podman tag docker.io/trading-trends/kafka:dev issue37-latency/kafka:dev
podman tag docker.io/trading-trends/viz:dev issue37-latency/viz:dev
python3.14 bench/run.py "$evidence/experiment.json" --no-build
