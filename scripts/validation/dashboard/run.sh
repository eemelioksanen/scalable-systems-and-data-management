#!/usr/bin/env bash
# End-to-end check of the dashboard (#13): replay -> Kafka -> Flink -> output topics -> WebSocket ->
# the frontend's reducer, against the topics and the oracle, with subscriptions changed mid-run.
#
#   docker compose down --volumes && docker compose up -d --build && docker compose build producer
#   oracle/run.sh data/daily/debs2022-gc-trading-day-0[89]-11-21.csv --out /out/mon-tue
#   scripts/validation/dashboard/run.sh [out-name]
#
# Results go to correctness/out/<out-name>/ (default dashboard-e2e). Needs only Docker and a fresh
# stack: the output topics must be empty. RATE (default 120) sets the replay speed, ORACLE the
# oracle directory (default oracle/out/mon-tue). See viz/README.md, "End-to-end check".
#
# Monday is sent unpaced, so the EMAs have a day of history; Tuesday is paced from 09:00. A first
# day alone has only each symbol's first-window BUY: crossovers need converged EMAs. The symbols
# are chosen to cross on Tuesday around the 12:00 subscription change.
set -euo pipefail

cd "$(dirname "$0")/../../.."
REPO="$PWD"
NAME="${1:-dashboard-e2e}"
OUT="correctness/out/$NAME"
RATE="${RATE:-120}"
ORACLE="${ORACLE:-oracle/out/mon-tue}"
INPUTS=(--input /data/daily/debs2022-gc-trading-day-08-11-21.csv --input /data/daily/debs2022-gc-trading-day-09-11-21.csv)
PACE_FROM="${PACE_FROM:-2021-11-09T09:00}"
NETWORK="${COMPOSE_NETWORK:-trading-trends_default}"
VIZ_PORT="${VIZ_HOST_PORT:-8088}"
SYMBOLS=RDSA.NL,ASML.NL,AIR.FR,A1CX3T.ETR,INVDS.FR,ISAMD.FR,IPLU3.FR,EUS5D.NL,I3BP.NL,A1EXV9.ETR,A0S3AS.ETR
# Mid-run, once the feed reaches this source time (local, at the job's zone offset).
CHANGE_AT="${CHANGE_AT:-2021-11-09T12:00}"

log() { echo "[$(date +%T)] $*" >&2; }
health() { curl -sf "localhost:$VIZ_PORT/health"; }
field() { python3 -c "import json,sys; v=json.load(sys.stdin).get('$1'); print('' if v is None else v)"; }

subscribe() {  # trader json|null
  if [[ "$2" == null ]]; then
    # A tombstone: the console producer writes a null value for the literal "null" with this property.
    printf '%s|null\n' "$1" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 --topic subscriptions --reader-property parse.key=true \
      --reader-property key.separator='|' --reader-property null.marker=null
  else
    printf '%s|%s\n' "$1" "$2" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 --topic subscriptions --reader-property parse.key=true \
      --reader-property key.separator='|'
  fi
}

capture() {  # name extra-args...
  local name="$1"; shift
  docker run --rm --network "$NETWORK" --user "$(id -u):$(id -g)" \
    -v "$REPO/viz/web:/web:ro" -v "$REPO/$OUT:/out" -w /web node:24-alpine \
    node scripts/capture.mjs --url ws://viz:8088/ws --out "/out/$name.json" "$@"
}

mkdir -p "$OUT"
[[ -f "$ORACLE/ema_results.parquet" ]] || { echo "no oracle output in $ORACLE" >&2; exit 1; }
[[ "$(health | field resultsRead)" == 0 ]] || { echo "the dashboard has already read results: reset the stack" >&2; exit 1; }

log "phase 1 subscriptions"
subscribe trader-a '{"trader":"trader-a","symbols":["ASML.NL","RDSA.NL","EUS5D.NL"]}'
subscribe trader-b '{"trader":"trader-b","symbols":["AIR.FR","INVDS.FR","A1EXV9.ETR"]}'
subscribe trader-x '{"trader":"trader-x","symbols":["ISAMD.FR","A1CX3T.ETR"]}'

log "submitting the job (live defaults)"
docker compose exec -T jobmanager flink run -d -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar \
  | tee "$OUT/submit.txt"
JOB=$(grep -o 'JobID [0-9a-f]*' "$OUT/submit.txt" | awk '{print $2}')

log "live capture, connected through the whole run, charting INVDS.FR"
rm -f "$OUT/drained"
capture live --symbols INVDS.FR --stop-file /out/drained --settle-ms 5000 --timeout-ms 3600000 &
LIVE=$!

log "replaying $SYMBOLS at ${RATE}x"
docker compose run --rm -T producer fi.aalto.cse4780.replay.Replayer "${INPUTS[@]}" --mode realtime --rate "$RATE" \
  --pace-from "$PACE_FROM" --symbols "$SYMBOLS" > "$OUT/replay.json" 2> "$OUT/replay.log" &
REPLAY=$!

change_ms=$(python3 -c "from datetime import datetime,timezone,timedelta; print(int(datetime.fromisoformat('$CHANGE_AT').replace(tzinfo=timezone(timedelta(hours=2))).timestamp()*1000))")
until [[ -n "$(health | field feedMs)" && "$(health | field feedMs)" -ge "$change_ms" ]]; do sleep 1; done
log "phase 2 subscriptions at feed time $CHANGE_AT"
# trader-b adds IPLU3.FR, which crosses at 15:40 and 16:55. Removing trader-x stops A1CX3T.ETR
# before its 16:55 SELL; trader-c keeps ISAMD.FR published and adds two symbols never published.
subscribe trader-b '{"trader":"trader-b","symbols":["AIR.FR","INVDS.FR","A1EXV9.ETR","IPLU3.FR"]}'
subscribe trader-x null
subscribe trader-c '{"symbols":["I3BP.NL","A0S3AS.ETR","ISAMD.FR"]}'
date +%s%3N > "$OUT/phase2-wall-ms.txt"
cat > "$OUT/traders.json" <<'EOF'
{"trader-a": ["ASML.NL", "EUS5D.NL", "RDSA.NL"], "trader-b": ["A1EXV9.ETR", "AIR.FR", "INVDS.FR", "IPLU3.FR"], "trader-c": ["A0S3AS.ETR", "I3BP.NL", "ISAMD.FR"]}
EOF

wait "$REPLAY"
log "replay finished: $(python3 -c "import json; r=json.load(open('$OUT/replay.json')); print(r['status'], r['counts']['acknowledged'], 'records')")"
log "draining the job: evaluates the windows still open"
docker compose exec -T jobmanager flink stop --drain --savepointPath file:///tmp/flink-savepoints "$JOB" > "$OUT/drain.txt"
sleep 5
touch "$OUT/drained"

log "exporting the topics"
correctness/run.sh export --zone-offset +02:00 --replay-report "$OUT/replay.json" --out "/out/$NAME/export" > "$OUT/export.json"
health > "$OUT/health.json"

log "waiting for the live capture to settle"
wait "$LIVE"
log "fresh capture, charting every published symbol"
capture final --symbols all --settle-ms 3000

for c in live final; do
  log "checking the $c capture"
  correctness/run.sh dashboard --capture "$OUT/$c.json" --job "$OUT/export" --oracle "$ORACLE" \
    --expect-traders "$OUT/traders.json" --out "/out/$NAME/check-$c" > "$OUT/check-$c.json" || true
  python3 -c "import json; r=json.load(open('$OUT/check-$c.json')); print('$c', 'PASS' if r['pass'] else 'FAIL', [c['name'] for c in r['checks'] if not c['pass']])"
done
log "late ticks: $(python3 -c "import json; m=json.load(open('$OUT/export/manifest.json')); print(m['topics']['late-ticks']['rows'])")"
