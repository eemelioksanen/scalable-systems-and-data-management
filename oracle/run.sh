#!/usr/bin/env bash
# Runs the oracle, or its tests, in the canonical container, or in a local venv with USE_VENV=1.
#
#   oracle/run.sh [options] data/debs2022-gc-trading-day-*.csv     # results in oracle/out/
#   oracle/run.sh test [pytest args]
#
# Day files must lie under DATA_DIR (default: data/), which the container mounts read-only.
# Other input files (e.g. --exclude-events) must lie inside the repository. In the container
# the output directory is oracle/out/; pass --out /out/<name> for a subdirectory of it.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO="$PWD"
DATA_DIR="$(realpath "${DATA_DIR:-data}")"

mode=run
if [[ "${1:-}" == "test" ]]; then
  mode=test
  shift
fi

ORACLE_GIT_REV="$(git rev-parse HEAD 2>/dev/null || true)"
ORACLE_GIT_DIRTY=0
[[ -z "$(git status --porcelain -- oracle fixtures 2>/dev/null)" ]] || ORACLE_GIT_DIRTY=1
export ORACLE_GIT_REV ORACLE_GIT_DIRTY

if [[ "${USE_VENV:-0}" == "1" ]]; then
  [[ -d oracle/.venv ]] || {
    # Pinned to an interpreter the wheels actually cover.
    py=""
    for c in python3.13 python3.12 python3.11 /usr/bin/python3; do
      command -v "$c" >/dev/null 2>&1 && { py="$c"; break; }
    done
    [[ -n "$py" ]] || { echo "no supported python found (need 3.11-3.13)" >&2; exit 1; }
    "$py" -m venv oracle/.venv
    oracle/.venv/bin/pip install --quiet -r oracle/requirements.txt
  }
  [[ "$mode" == test ]] && exec oracle/.venv/bin/python3 -m pytest oracle "$@"
  PYTHONPATH=oracle exec oracle/.venv/bin/python3 -m ema_oracle "$@"
fi

# Translate existing file arguments to the paths the container sees.
args=()
for a in "$@"; do
  if [[ -f "$a" ]]; then
    r="$(realpath "$a")"
    if [[ "$r" == "$DATA_DIR"/* ]]; then
      args+=("/data/${r#"$DATA_DIR"/}")
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

if [[ "$mode" == test ]]; then
  cmd=(-m pytest "${args[@]}")
else
  cmd=(-m ema_oracle "${args[@]}")
fi

data_mount=()
[[ -d "$DATA_DIR" ]] && data_mount=(-v "$DATA_DIR:/data:ro")

mkdir -p oracle/out
docker build --quiet -t trading-trends/oracle:dev oracle >/dev/null
exec docker run --rm \
  --read-only \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  --network none \
  --user "$(id -u):$(id -g)" \
  -e ORACLE_GIT_REV -e ORACLE_GIT_DIRTY \
  -v "$REPO/oracle:/oracle:ro" \
  -v "$REPO/fixtures:/fixtures:ro" \
  -v "$REPO:/repo:ro" \
  "${data_mount[@]}" \
  -v "$REPO/oracle/out:/out" \
  --tmpfs /tmp:size=2g \
  trading-trends/oracle:dev "${cmd[@]}"
