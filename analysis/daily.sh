#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
day="${1:?usage: bash analysis/daily.sh YYYY-MM-DD}"
shift
file=$(python3 - "$day" <<'PY'
import datetime, sys
day = datetime.date.fromisoformat(sys.argv[1])
if not datetime.date(2021, 11, 8) <= day <= datetime.date(2021, 11, 14):
    raise SystemExit("day must be within 2021-11-08 through 2021-11-14")
print(f"debs2022-gc-trading-day-{day:%d-%m-%y}.csv")
PY
)
python3 analysis/src/fetch_day.py "$day"
mkdir -p data/analysis analysis/out/daily
docker build -q -t trading-trends/analysis:issue4 analysis
docker run --rm --memory=4g --cpus=4 --read-only --network none \
    --security-opt no-new-privileges:true --cap-drop ALL \
    -e POLARS_MAX_THREADS=2 -e ANALYSIS_REVISION="$(git rev-parse HEAD)" \
    -e ANALYSIS_WORKTREE_STATUS="$(git status --porcelain)" \
    -v "$PWD/analysis/src:/src:ro" -v "$PWD/data/daily:/data:ro" \
    -v "$PWD/data/analysis:/cache" -v "$PWD/analysis/out/daily:/out" \
    --tmpfs /tmp:size=128m \
    trading-trends/analysis:issue4 /src/daily.py "/data/$file" \
    --day "$day" --cache /cache --out /out "$@"
