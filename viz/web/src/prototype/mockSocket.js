// PROTOTYPE: replays docs/validation/2026-11-10-dashboard/capture-final.json as the backend's
// WebSocket protocol, one window per tick, so the variants can be judged without Kafka or Flink.
// It also sends `quotes` (latest close, EMAs, span-open close and a close sparkline per symbol),
// which the real backend does not publish yet.
import capture from './capture.json';

const SLOTS = capture.slots;
const SLOT_MS = capture.slotMs;
const TICK_MS = 500;
const START_AT = Date.UTC(2021, 10, 9, 14, 0);

const advisories = capture.feed.map((a) => [a.symbol, a.windowEnd, a.type, a.secType, a.exchange, a.ema38, a.ema100,
  a.latencyMs == null ? null : 0, a.latencyMs]);
const lastEnd = capture.spanEnd;

function cellsUpTo(row, t) {
  return row.cells.filter(([w]) => w <= t && w > t - SLOTS * SLOT_MS);
}

function lastFlip(cells) {
  let flip = null;
  let prev = null;
  for (const [w, s] of cells) {
    if (s !== prev) flip = w;
    prev = s;
  }
  return flip;
}

function cellString(row, t) {
  const out = new Array(SLOTS).fill('.');
  for (const [w, s] of cellsUpTo(row, t)) out[SLOTS - 1 - Math.round((t - w) / SLOT_MS)] = s;
  return out.join('');
}

function pointsUpTo(symbol, t) {
  return (capture.series[symbol]?.points ?? []).filter((p) => p[0] <= t && p[0] > t - SLOTS * SLOT_MS);
}

function quotes(t) {
  const out = {};
  for (const row of capture.rows) {
    const pts = pointsUpTo(row.symbol, t);
    if (!pts.length) continue;
    const last = pts.at(-1);
    out[row.symbol] = {
      close: last[1], ema38: last[2], ema100: last[3], open: pts[0][1], at: last[0],
      spark: pts.slice(-60).map((p) => p[1]),
      spark38: pts.slice(-60).map((p) => p[2]),
    };
  }
  return out;
}

function clock(t) {
  return { wallMs: Date.now(), feedMs: t + 140000, feedIdleSince: null, resultsMs: t, zoneOffset: capture.clock.zoneOffset };
}

function visibleRows(t) {
  return capture.rows.filter((r) => r.cells[0][0] <= t);
}

export class MockSocket {
  constructor() {
    this.readyState = 0;
    this.selected = null;
    this.t = START_AT;
    setTimeout(() => {
      this.readyState = 1;
      this.onopen?.();
      this.snapshot();
      this.timer = setInterval(() => this.step(), TICK_MS);
    }, 120);
  }

  emit(msg) {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }

  snapshot() {
    const t = this.t;
    this.emit({
      type: 'snapshot', spanEnd: t, slots: SLOTS, slotMs: SLOT_MS,
      symbols: visibleRows(t).map((r) => ({ symbol: r.symbol, secType: r.secType, exchange: r.exchange,
        lastFlip: lastFlip(cellsUpTo(r, t)), cells: cellString(r, t) })),
      series: null,
      advisories: advisories.filter((a) => a[1] <= t),
      traders: capture.traders,
      clock: clock(t),
      quotes: quotes(t),
    });
    if (this.selected) this.sendSeries();
  }

  sendSeries() {
    const s = this.selected;
    this.emit({ type: 'series', series: { symbol: s, firstWindowEnd: capture.series[s]?.firstWindowEnd ?? null, points: pointsUpTo(s, this.t) } });
  }

  step() {
    if (this.t >= lastEnd) {
      this.t = START_AT;
      this.snapshot();
      return;
    }
    const t = (this.t += SLOT_MS);
    const cells = [];
    for (const r of visibleRows(t)) {
      const c = r.cells.find(([w]) => w === t);
      if (c) cells.push([r.symbol, t, c[1], r.secType, r.exchange, lastFlip(cellsUpTo(r, t))]);
    }
    const sel = this.selected && pointsUpTo(this.selected, t).filter((p) => p[0] === t);
    this.emit({
      type: 'delta', spanEnd: t, cells,
      points: sel?.length ? { symbol: this.selected, points: sel } : null,
      advisories: advisories.filter((a) => a[1] === t),
      traders: null,
      clock: clock(t),
      quotes: quotes(t),
    });
  }

  send(text) {
    const msg = JSON.parse(text);
    if (msg.type === 'select') {
      this.selected = msg.symbol;
      this.sendSeries();
    }
  }

  close() {
    clearInterval(this.timer);
    this.readyState = 3;
  }
}
