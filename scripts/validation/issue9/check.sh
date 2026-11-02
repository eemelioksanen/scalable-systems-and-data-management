#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
: "${ISSUE9_DATASET:?Set ISSUE9_DATASET to the directory containing the seven complete daily files}"
: "${ISSUE9_PYTHON:?Set ISSUE9_PYTHON to Python 3.11+ with correctness/requirements.txt installed}"
export PYTHONPATH=correctness:oracle
export PYTHONDONTWRITEBYTECODE=1
bash scripts/validation/issue9/prepare.sh correctness/out/issue9
inputs=()
for day in 08 09 10 11 12 13 14; do inputs+=("debs2022-gc-trading-day-$day-11-21.csv"); done
python3 scripts/validation/issue9/inputs.py "$ISSUE9_DATASET"
python3 scripts/validation/issue9/run.py oracle --name full-week --dataset "$ISSUE9_DATASET" --input "${inputs[@]}"
for run in week-1 week-2; do
  python3 scripts/validation/issue9/run.py run --name "$run" --dataset "$ISSUE9_DATASET" --input "${inputs[@]}" --oracle full-week
done
"$ISSUE9_PYTHON" scripts/validation/issue9/fixtures.py
for run in semantics disorder; do
  python3 scripts/validation/issue9/run.py run --name "$run" --dataset "data/issue9/$run" --input trading-day-08-11-21.csv trading-day-09-11-21.csv --bound 86400000 --oracle "$run"
done
python3 scripts/validation/issue9/run.py run --name subset --dataset data/issue9/semantics --input trading-day-08-11-21.csv trading-day-09-11-21.csv --bound 86400000 --symbols EDGE.NL SWING.FR
python3 scripts/validation/issue9/idle.py
"$ISSUE9_PYTHON" scripts/validation/issue9/semantics.py
"$ISSUE9_PYTHON" scripts/validation/issue9/controls.py
"$ISSUE9_PYTHON" scripts/validation/issue9/report.py
