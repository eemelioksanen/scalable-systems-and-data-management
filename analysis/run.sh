#!/usr/bin/env bash
# Runs a profiling script in the canonical container, or in a local venv with USE_VENV=1.
# Both paths must produce identical output; the container is what the report cites.
set -euo pipefail

cd "$(dirname "$0")/.."
script="${1:?usage: analysis/run.sh <script.py> [args...]}"
shift || true

if [[ "${USE_VENV:-0}" == "1" ]]; then
  [[ -d analysis/.venv ]] || {
    # Pinned to an interpreter the wheels actually cover: the newest CPython
    # releases routinely lag on binary wheels and fall back to source builds.
    py=""
    for c in python3.13 python3.12 python3.11 /usr/bin/python3; do
      command -v "$c" >/dev/null 2>&1 && { py="$c"; break; }
    done
    [[ -n "$py" ]] || { echo "no supported python found (need 3.11-3.13)" >&2; exit 1; }
    "$py" -m venv analysis/.venv
    analysis/.venv/bin/pip install --quiet -r analysis/requirements.txt
  }
  exec analysis/.venv/bin/python3 "$script" "$@"
fi

docker build --quiet -t trading-trends/analysis:dev analysis >/dev/null
exec docker run --rm \
  --read-only \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  --network none \
  -v "$PWD/analysis/src:/src:ro" \
  -v "$PWD/data:/data:ro" \
  -v "$PWD/analysis/out:/out" \
  -v "$PWD/fixtures:/fixtures" \
  --tmpfs /tmp:size=2g \
  trading-trends/analysis:dev "/src/$(basename "$script")" "$@"
