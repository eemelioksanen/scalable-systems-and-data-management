// Dashboard state, patched by the WebSocket messages of viz/README.md.
// The heatmap lives in one Int8Array of symbols x slots, indexed by
// slot = (windowEnd / slotMs) mod slots, so a moving span only clears the slots it leaves.

export const NONE = 0;
export const UP = 1;
export const DOWN = 2;
export const EQUAL = 3;

const SIGN = { '+': UP, '-': DOWN, '0': EQUAL, '.': NONE };

export const initialState = {
  received: false,
  spanEnd: null,
  slots: 288,
  slotMs: 300000,
  rows: [],
  index: new Map(),
  signs: new Int8Array(0),
  version: 0,
  series: null,
  // Advisories in the span, keyed by advisoryKey; advVersion moves whenever they change.
  advisories: new Map(),
  advVersion: 0,
  // Each trader's current list from the subscriptions topic; ['*'] means every symbol.
  traders: {},
  clock: null,
};

export const advisoryKey = (symbol, windowEnd) => `${symbol}|${windowEnd}`;

// A symbol's first window turns EMA38 = close·2/39 and EMA100 = close·2/101, from EMA0 = 0
// (D-7), so its BUY comes from the zero start, not from a breakout. The ratio of the two EMAs
// identifies it from the record alone: 101/39 for every close.
export function isFirstWindowBuy(type, ema38, ema100) {
  return type === 'BUY' && Math.abs(ema38 * 39 - ema100 * 101) <= 1e-9 * Math.abs(ema100 * 101);
}

// [symbol, windowEnd, type, secType, exchange, ema38, ema100, maxIngestTs, emitTs], as the backend
// sends them. latencyMs is the advisory's wall-clock time from admission to publication (D-40).
function toAdvisory([symbol, windowEnd, type, secType, exchange, ema38, ema100, maxIngestTs = null, emitTs = null]) {
  return {
    symbol, windowEnd, type, secType, exchange, ema38, ema100,
    latencyMs: maxIngestTs == null || emitTs == null ? null : emitTs - maxIngestTs,
    firstWindow: isFirstWindowBuy(type, ema38, ema100),
  };
}

function mergeAdvisories(map, list) {
  for (const a of list) {
    const adv = toAdvisory(a);
    map.set(advisoryKey(adv.symbol, adv.windowEnd), adv);
  }
}

function trimAdvisories(map, spanEnd, slots, slotMs) {
  if (spanEnd == null) return false;
  const start = spanEnd - slots * slotMs;
  let removed = false;
  for (const [k, a] of map) {
    if (a.windowEnd <= start) {
      map.delete(k);
      removed = true;
    }
  }
  return removed;
}

export function slotOf(windowEnd, slots, slotMs) {
  const s = Math.floor(windowEnd / slotMs) % slots;
  return s < 0 ? s + slots : s;
}

export function spanStart(state) {
  return state.spanEnd == null ? null : state.spanEnd - state.slots * state.slotMs;
}

export function applySnapshot(_state, msg) {
  const { slots, slotMs, spanEnd } = msg;
  const rows = [];
  const index = new Map();
  const signs = new Int8Array(Math.max(16, msg.symbols.length) * slots);
  msg.symbols.forEach((s, r) => {
    rows.push({ symbol: s.symbol, secType: s.secType, exchange: s.exchange, lastFlip: s.lastFlip });
    index.set(s.symbol, r);
    if (spanEnd == null) return;
    for (let i = 0; i < slots; i++) {
      const v = SIGN[s.cells[i]] ?? NONE;
      if (v !== NONE) {
        const windowEnd = spanEnd - (slots - 1 - i) * slotMs;
        signs[r * slots + slotOf(windowEnd, slots, slotMs)] = v;
      }
    }
  });
  return {
    received: true,
    spanEnd,
    slots,
    slotMs,
    rows,
    index,
    signs,
    version: _state.version + 1,
    series: msg.series ? trimSeries(msg.series, spanEnd, slots, slotMs) : null,
    advisories: snapshotAdvisories(msg.advisories ?? []),
    advVersion: _state.advVersion + 1,
    traders: msg.traders ?? {},
    clock: msg.clock,
  };
}

function snapshotAdvisories(list) {
  const map = new Map();
  mergeAdvisories(map, list);
  return map;
}

export function applySeries(state, msg) {
  return { ...state, series: trimSeries(msg.series, state.spanEnd, state.slots, state.slotMs) };
}

export function applyDelta(state, msg) {
  const { slots, slotMs } = state;
  let { rows, index, signs } = state;
  if (msg.spanEnd != null && state.spanEnd != null && msg.spanEnd > state.spanEnd) {
    clearSlots(signs, rows.length, slots, slotMs, state.spanEnd, msg.spanEnd);
  }
  const spanEnd = msg.spanEnd ?? state.spanEnd;
  if (msg.cells.length > 0) {
    rows = rows.slice();
    index = new Map(index);
    for (const [symbol, windowEnd, sign, secType, exchange, lastFlip] of msg.cells) {
      let r = index.get(symbol);
      if (r === undefined) {
        r = rows.length;
        rows.push({ symbol, secType, exchange, lastFlip });
        index.set(symbol, r);
        if ((r + 1) * slots > signs.length) {
          const grown = new Int8Array(Math.max(signs.length * 2, (r + 1) * slots));
          grown.set(signs);
          signs = grown;
        }
      } else {
        rows[r] = { ...rows[r], secType, exchange, lastFlip };
      }
      if (spanEnd != null && windowEnd > spanEnd - slots * slotMs) {
        signs[r * slots + slotOf(windowEnd, slots, slotMs)] = SIGN[sign] ?? NONE;
      }
    }
  }
  let series = state.series;
  if (msg.points && series && msg.points.symbol === series.symbol) {
    series = { ...series, points: mergePoints(series.points, msg.points.points) };
  }
  if (series && spanEnd !== state.spanEnd) {
    series = trimSeries(series, spanEnd, slots, slotMs);
  }
  let { advisories, advVersion } = state;
  const incoming = msg.advisories ?? [];
  if (incoming.length > 0 || spanEnd !== state.spanEnd) {
    const next = new Map(advisories);
    mergeAdvisories(next, incoming);
    const trimmed = spanEnd !== state.spanEnd && trimAdvisories(next, spanEnd, slots, slotMs);
    if (incoming.length > 0 || trimmed) {
      advisories = next;
      advVersion += 1;
    }
  }
  return {
    ...state,
    received: true,
    spanEnd,
    rows,
    index,
    signs,
    version: state.version + 1,
    series,
    advisories,
    advVersion,
    traders: msg.traders ?? state.traders,
    clock: msg.clock ?? state.clock,
  };
}

// The advisories in three shapes: all of them newest first (the feed), by symbol oldest first
// (chart markers and heatmap ticks), and each symbol's latest (the heatmap's crossover sort).
// lastCross skips first-window BUYs unless they are shown: they are no breakout.
export function indexAdvisories(advisories, { firstWindows = false } = {}) {
  const all = [...advisories.values()].sort((a, b) => b.windowEnd - a.windowEnd
    || (a.symbol < b.symbol ? -1 : a.symbol > b.symbol ? 1 : 0));
  const bySymbol = new Map();
  const lastCross = new Map();
  for (let i = all.length - 1; i >= 0; i--) {
    const a = all[i];
    let list = bySymbol.get(a.symbol);
    if (!list) bySymbol.set(a.symbol, (list = []));
    list.push(a);
    if (firstWindows || !a.firstWindow) lastCross.set(a.symbol, a);
  }
  return { all, bySymbol, lastCross };
}

// The symbols a trader's current list covers: null for no trader filter, 'all' for '*'.
export function traderScope(traders, trader) {
  if (!trader) return null;
  const list = traders[trader];
  if (!list) return new Set();
  return list.includes('*') ? 'all' : new Set(list);
}

// Whether a heatmap row or an advisory passes the exchange, SecType and subscription filters.
export function passes(item, filters, scope) {
  if (filters.exchange && item.exchange !== filters.exchange) return false;
  if (filters.secType && item.secType !== filters.secType) return false;
  if (scope && scope !== 'all' && !scope.has(item.symbol)) return false;
  return true;
}

// The advisory feed: newest first, after the filters. First-window BUYs only with filters.firstWindows.
export function feedItems(all, filters, traders) {
  const scope = traderScope(traders, filters.trader);
  return all.filter((a) => (filters.firstWindows || !a.firstWindow) && passes(a, filters, scope));
}

// Clears the slots of windows that left the span when it moved from oldEnd to newEnd.
function clearSlots(signs, rowCount, slots, slotMs, oldEnd, newEnd) {
  const steps = Math.min(slots, Math.round((newEnd - oldEnd) / slotMs));
  for (let k = 1; k <= steps; k++) {
    const slot = slotOf(oldEnd + k * slotMs, slots, slotMs);
    for (let r = 0; r < rowCount; r++) signs[r * slots + slot] = NONE;
  }
}

export function mergePoints(points, updates) {
  const byEnd = new Map(points.map((p) => [p[0], p]));
  for (const p of updates) byEnd.set(p[0], p);
  return [...byEnd.values()].sort((a, b) => a[0] - b[0]);
}

function trimSeries(series, spanEnd, slots, slotMs) {
  if (spanEnd == null) return series;
  const start = spanEnd - slots * slotMs;
  return { ...series, points: series.points.filter((p) => p[0] > start) };
}

// The sign of one heatmap cell; column 0 is the oldest slot of the span.
export function cellAt(state, row, column) {
  if (state.spanEnd == null) return NONE;
  const windowEnd = state.spanEnd - (state.slots - 1 - column) * state.slotMs;
  return state.signs[row * state.slots + slotOf(windowEnd, state.slots, state.slotMs)];
}

// Row indices after the filters. sort is 'cross' (newest published advisory first), 'flip'
// (newest sign flip of EMA38 - EMA100 first) or 'symbol'; ties and missing values go by symbol.
export function visibleRows(rows, filters, { sort = 'flip', lastCross = new Map(), traders = {} } = {}) {
  const scope = traderScope(traders, filters.trader);
  const out = [];
  rows.forEach((row, i) => {
    if (passes(row, filters, scope)) out.push(i);
  });
  const key = (r) => {
    if (sort === 'cross') return lastCross.get(rows[r].symbol)?.windowEnd ?? null;
    if (sort === 'flip') return rows[r].lastFlip;
    return null;
  };
  out.sort((a, b) => {
    const fa = key(a);
    const fb = key(b);
    if (fa !== fb) {
      if (fa == null) return 1;
      if (fb == null) return -1;
      return fb - fa;
    }
    return rows[a].symbol < rows[b].symbol ? -1 : rows[a].symbol > rows[b].symbol ? 1 : 0;
  });
  return out;
}

// uPlot data in seconds. A step longer than one slot gets a null point, so the lines break
// where windows had no trades instead of interpolating across them (D-6).
export function toChartData(points, slotMs) {
  const xs = [];
  const close = [];
  const ema38 = [];
  const ema100 = [];
  points.forEach((p, i) => {
    if (i > 0 && p[0] - points[i - 1][0] > slotMs) {
      xs.push((points[i - 1][0] + slotMs) / 1000);
      close.push(null);
      ema38.push(null);
      ema100.push(null);
    }
    xs.push(p[0] / 1000);
    close.push(p[1]);
    ema38.push(p[2]);
    ema100.push(p[3]);
  });
  return [xs, close, ema38, ema100];
}
