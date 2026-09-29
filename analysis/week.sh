#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/analysis analysis/out/week
docker build -q -t trading-trends/analysis:issue4 analysis
docker run --rm --memory=4g --cpus=4 --read-only --network none \
    --security-opt no-new-privileges:true --cap-drop ALL \
    -e POLARS_MAX_THREADS=2 -e ANALYSIS_REVISION="$(git rev-parse HEAD)" \
    -e ANALYSIS_WORKTREE_STATUS="$(git status --porcelain)" \
    -v "$PWD/analysis/src:/src:ro" -v "$PWD/oracle:/oracle:ro" \
    -v "$PWD/data/analysis:/cache" -v "$PWD/analysis/out:/out" \
    --tmpfs /tmp:size=128m \
    trading-trends/analysis:issue4 /src/week.py --cache /cache \
    --daily-out /out/daily --out /out/week --oracle /oracle "$@"
