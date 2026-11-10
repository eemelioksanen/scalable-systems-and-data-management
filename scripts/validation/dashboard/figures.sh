#!/usr/bin/env bash
# Captures the running dashboard for the report with headless Chromium (viz/README.md, Figures).
#
#   scripts/validation/dashboard/figures.sh [out-dir]     # default report/figures
#
# Run it after scripts/validation/dashboard/run.sh, while the stack is still up, so the figures
# show the checked run. Needs Node 22+ and a Chromium or Chrome on the host: CHROME names it
# (default chromium), VIZ_HOST_PORT the dashboard's port. Each view is fixed by its query string
# and hash, so the same data gives the same figure.
set -euo pipefail

cd "$(dirname "$0")/../../.."
OUT="$(realpath -m "${1:-report/figures}")"
BASE="http://localhost:${VIZ_HOST_PORT:-8088}"
mkdir -p "$OUT"

shoot() {  # name path
  node viz/web/scripts/screenshot.mjs --url "$BASE$2" --out "$OUT/$1.png" --width 1600 --height 900
}

# The whole dashboard: heatmap by latest crossover, INVDS.FR charted with its SELL and BUY, the feed.
shoot dashboard-overview '/#INVDS.FR'
# One trader's view: trader-b added IPLU3.FR at 12:00, before its 15:40 SELL and 16:55 BUY.
shoot dashboard-trader '/?trader=trader-b#IPLU3.FR'
