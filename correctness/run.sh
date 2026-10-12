#!/usr/bin/env bash
# Runs the correctness tools, or their tests, in a container, or in a local venv with USE_VENV=1.
#
#   correctness/run.sh export --zone-offset +02:00 --out /out/<name>     # from the Compose stack's Kafka
#   correctness/run.sh compare --oracle oracle/out --job correctness/out/<export> --out /out/<name> ...
#   correctness/run.sh inject data/daily/<file>.csv ... --out /data/injected/<name> ...
#   correctness/run.sh test [pytest args]
#
# Day files must lie under DATA_DIR (default: data/), which the container mounts read-only
# except for data/injected/, where inject writes so that the replayer can read its output.
# Other inputs must lie inside the repository. Results go to correctness/out/, which the
# container sees as /out.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO="$PWD"
DATA_DIR="$(cd "${DATA_DIR:-data}" 2>/dev/null && pwd -P || echo "$REPO/data")"
command="${1:?usage: correctness/run.sh export|compare|inject|test ...}"
shift

if [[ "${USE_VENV:-0}" == "1" ]]; then
  [[ -d correctness/.venv ]] || {
    py=""
    for c in python3.13 python3.12 python3.11; do
      command -v "$c" >/dev/null 2>&1 && { py="$c"; break; }
    done
    [[ -n "$py" ]] || { echo "no supported python found (need 3.11-3.13)" >&2; exit 1; }
    "$py" -m venv correctness/.venv
    correctness/.venv/bin/pip install --quiet -r correctness/requirements.txt
  }
  [[ "$command" == test ]] && exec correctness/.venv/bin/python3 -m pytest correctness "$@"
  PYTHONPATH=correctness:oracle exec correctness/.venv/bin/python3 -m ema_check "$command" "$@"
fi

# Translate existing paths to the ones the container sees.
args=()
for a in "$@"; do
  if [[ -e "$a" ]]; then
    r="$(realpath "$a")"
    if [[ "$r" == "$DATA_DIR"/* ]]; then
      args+=("/data/${r#"$DATA_DIR"/}")
    elif [[ "$r" == "$REPO/correctness/out"/* ]]; then
      args+=("/out/${r#"$REPO/correctness/out"/}")
    elif [[ "$r" == "$REPO"/* ]]; then
      args+=("/repo/${r#"$REPO"/}")
    else
      echo "$a is neither under DATA_DIR ($DATA_DIR) nor in the repository" >&2
      exit 1
    fi
  else
    args+=("$a")
  fi
done

if [[ "$command" == test ]]; then
  cmd=(-m pytest /correctness ${args[@]+"${args[@]}"})
else
  cmd=(-m ema_check "$command" ${args[@]+"${args[@]}"})
fi

network=(--network none)
[[ "$command" == export ]] && network=(--network "${COMPOSE_NETWORK:-trading-trends_default}" -e BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:9092}")

data_mount=()
if [[ -d "$DATA_DIR" ]]; then
  data_mount=(-v "$DATA_DIR:/data:ro")
  if [[ "$command" == inject ]]; then
    mkdir -p "$DATA_DIR/injected"
    data_mount+=(-v "$DATA_DIR/injected:/data/injected")
  fi
fi

mkdir -p correctness/out
docker build --quiet -t trading-trends/correctness:dev correctness >/dev/null
exec docker run --rm \
  --read-only \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  "${network[@]}" \
  --user "$(id -u):$(id -g)" \
  -v "$REPO/correctness:/correctness:ro" \
  -v "$REPO/oracle:/oracle:ro" \
  -v "$REPO/fixtures:/fixtures:ro" \
  -v "$REPO:/repo:ro" \
  ${data_mount[@]+"${data_mount[@]}"} \
  -v "$REPO/correctness/out:/out" \
  --tmpfs /tmp:size=2g \
  trading-trends/correctness:dev "${cmd[@]}"
