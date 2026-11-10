// Connects to the dashboard like a browser, applies every message with the frontend's own reducer
// (src/state.js), and writes what the views would show as JSON for the end-to-end check
// (viz/README.md, "End-to-end check"). Needs Node 22 or later for the global WebSocket.
//
//   node scripts/capture.mjs --url ws://localhost:8088/ws --symbols RDSA.NL,ASML.NL --out capture.json
//
// --symbols all charts every published symbol in turn. The capture ends once no message has
// changed the data for --settle-ms, after every symbol's series has arrived and --min-ms has passed.
// Only the last symbol selected keeps receiving points, so chart several symbols only after the run.

import { existsSync, writeFileSync } from 'node:fs';
import { formatDateTime } from '../src/format.js';
import {
  DOWN, EQUAL, UP, applyDelta, applySeries, applySnapshot, cellAt, feedItems, indexAdvisories, initialState,
  visibleRows,
} from '../src/state.js';

const args = Object.fromEntries(process.argv.slice(2).reduce((out, a, i, all) => {
  if (a.startsWith('--')) out.push([a.slice(2), all[i + 1]]);
  return out;
}, []));
const url = args.url ?? 'ws://localhost:8088/ws';
const settleMs = Number(args['settle-ms'] ?? 3000);
// Stay connected at least this long, e.g. through a live replay, so the deltas build the state.
const minMs = Number(args['min-ms'] ?? 0);
// Or until this file exists: the run's script creates it once the job has drained.
const stopFile = args['stop-file'];
const timeoutMs = Number(args['timeout-ms'] ?? 120000);
const out = args.out ?? 'capture.json';

const SIGN = { [UP]: '+', [DOWN]: '-', [EQUAL]: '0' };

let state = initialState;
let lastChange = Date.now();
const series = {};
const pending = [];
let waitingFor = null;

const ws = new WebSocket(url);

function reduce(msg) {
  if (msg.type === 'snapshot') state = applySnapshot(state, msg);
  else if (msg.type === 'delta') state = applyDelta(state, msg);
  else if (msg.type === 'series') state = applySeries(state, msg);
  const dataChanged = msg.type !== 'delta' || msg.cells.length > 0 || msg.points || msg.advisories?.length > 0 || msg.traders;
  if (dataChanged) lastChange = Date.now();
}

function selectNext() {
  waitingFor = pending.shift() ?? null;
  if (waitingFor) ws.send(JSON.stringify({ type: 'select', symbol: waitingFor }));
}

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  reduce(msg);
  if (msg.type === 'snapshot' && pending.length === 0 && waitingFor == null) {
    const wanted = args.symbols === 'all' ? state.rows.map((r) => r.symbol) : (args.symbols ?? '').split(',').filter(Boolean);
    pending.push(...wanted);
    selectNext();
  }
  // A selected symbol's later points arrive in deltas and are merged into state.series.
  if (state.series && state.series.symbol === waitingFor && msg.type === 'series') {
    selectNext();
  }
  if (state.series) series[state.series.symbol] = state.series;
};

function dump() {
  const adv = indexAdvisories(state.advisories);
  const zone = state.clock?.zoneOffset ?? '+02:00';
  const rows = state.rows.map((row, r) => {
    const cells = [];
    for (let c = 0; c < state.slots; c++) {
      const v = cellAt(state, r, c);
      if (SIGN[v]) cells.push([state.spanEnd - (state.slots - 1 - c) * state.slotMs, SIGN[v]]);
    }
    return { ...row, cells };
  });
  const feeds = {};
  for (const trader of Object.keys(state.traders)) {
    feeds[trader] = feedItems(adv.all, { trader }, state.traders).map((a) => [a.symbol, a.windowEnd]);
  }
  const charted = {};
  for (const [symbol, s] of Object.entries(series)) {
    charted[symbol] = {
      firstWindowEnd: s.firstWindowEnd,
      points: s.points,
      markers: (adv.bySymbol.get(symbol) ?? []).map((a) => [a.windowEnd, a.type, a.ema38]),
    };
  }
  const order = visibleRows(state.rows, {}, { sort: 'cross', lastCross: adv.lastCross });
  return {
    url,
    capturedAtMs: Date.now(),
    spanEnd: state.spanEnd,
    slots: state.slots,
    slotMs: state.slotMs,
    clock: state.clock,
    traders: state.traders,
    rows,
    heatmapByCrossover: order.map((i) => state.rows[i].symbol),
    // The feed's rows, newest first, with the time column as the browser shows it.
    feed: adv.all.map((a) => ({ ...a, shown: formatDateTime(a.windowEnd, zone).slice(0, -3) })),
    feeds,
    series: charted,
  };
}

const started = Date.now();
const timer = setInterval(() => {
  const done = state.received && waitingFor == null && pending.length === 0 && Date.now() - lastChange >= settleMs
    && Date.now() - started >= minMs && (!stopFile || existsSync(stopFile));
  if (!done && Date.now() - started < Math.max(timeoutMs, minMs)) return;
  clearInterval(timer);
  const result = dump();
  writeFileSync(out, JSON.stringify(result));
  console.error(`${done ? 'settled' : 'timed out'}: ${result.rows.length} symbols, ${result.feed.length} advisories, `
    + `${Object.keys(result.series).length} series, ${Object.keys(result.traders).length} traders -> ${out}`);
  ws.close();
  process.exit(done ? 0 : 1);
}, 200);

ws.onerror = (e) => {
  console.error(`WebSocket error: ${e.message ?? e}`);
  process.exit(2);
};
