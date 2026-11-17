#!/usr/bin/env bash
# Runs every design-comparison variant (D-42) through bench/run.py on the hand-derived semantics
# fixtures and compares each with its oracle reference. It uses its own Compose project, image tags
# and host ports, so it leaves the default trading-trends stack alone.
#
#   scripts/validation/issue23/run.sh [config ...]     # default: every config in configs/
set -euo pipefail
cd "$(dirname "$0")/../../.."

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-issue23}"
export COMPOSE_NETWORK="${COMPOSE_PROJECT_NAME}_default"
export FLINK_HOST_PORT="${FLINK_HOST_PORT:-18081}" PROMETHEUS_HOST_PORT="${PROMETHEUS_HOST_PORT:-19090}"
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-39092}" GRAFANA_HOST_PORT="${GRAFANA_HOST_PORT:-13000}"
export VIZ_HOST_PORT="${VIZ_HOST_PORT:-18088}"
python="${PYTHON:-python3}"

mkdir -p data/issue23/semantics
cp fixtures/semantics/trading-day-*.csv data/issue23/semantics/
inputs=(data/issue23/semantics/trading-day-08-11-21.csv data/issue23/semantics/trading-day-09-11-21.csv)
for policy in skip carry-forward; do
  oracle/run.sh "${inputs[@]}" --zone-offset +02:00 --empty-windows "$policy" --out "/out/issue23-$policy" > /dev/null
done
oracle/run.sh "${inputs[@]}" --zone-offset +02:00 --watermark-bound-ms 1000 --out /out/issue23-bound-1s > /dev/null

configs=("$@")
[[ ${#configs[@]} -gt 0 ]] || configs=(scripts/validation/issue23/configs/*.json)
build=()
status=0
for config in "${configs[@]}"; do
  "$python" bench/run.py "$config" ${build[@]+"${build[@]}"} || status=1
  build=(--no-build)
  name="$("$python" -c 'import json, sys; print(json.load(open(sys.argv[1]))["name"])' "$config")"
  experiment="$(ls -d bench/results/raw/"$name"-* | tail -1)"
  docker compose logs --no-color taskmanager > "$experiment/taskmanager.log" 2>&1 || true
done
docker compose down --volumes --remove-orphans
exit "$status"
