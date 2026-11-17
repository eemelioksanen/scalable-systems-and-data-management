import { describe, expect, it } from 'vitest';
import {
  DOWN, EQUAL, NONE, UP, applyDelta, applySeries, applySnapshot, cellAt, feedItems, indexAdvisories, initialState,
  breadth, isFirstWindowBuy,
  mergePoints, quantile, spreadPct, toChartData, trendOf, traderScope, visibleRows,
} from './state.js';

const SLOT = 300000;
const T0 = 1636355100000;
const w = (i) => T0 + i * SLOT;
const clock = { wallMs: 0, feedMs: null, feedIdleSince: null, resultsMs: T0, zoneOffset: '+02:00' };

function cells(spec) {
  const out = Array(288).fill('.');
  for (const [column, c] of Object.entries(spec)) out[Number(column)] = c;
  return out.join('');
}

function snapshot(symbols, spanEnd = w(287), series = null) {
  return applySnapshot(initialState, { type: 'snapshot', spanEnd, slots: 288, slotMs: SLOT, symbols, series, clock });
}

describe('snapshot', () => {
  it('places each character at its column, oldest first', () => {
    const s = snapshot([{ symbol: 'A.FR', secType: 'E', exchange: 'FR', lastFlip: w(287), cells: cells({ 0: '0', 285: '-', 287: '+' }) }]);
    expect(cellAt(s, 0, 0)).toBe(EQUAL);
    expect(cellAt(s, 0, 285)).toBe(DOWN);
    expect(cellAt(s, 0, 286)).toBe(NONE);
    expect(cellAt(s, 0, 287)).toBe(UP);
    expect(s.received).toBe(true);
  });

  it('handles an empty topic', () => {
    const s = snapshot([], null);
    expect(s.rows).toEqual([]);
    expect(s.spanEnd).toBeNull();
  });
});

describe('delta', () => {
  const base = () => snapshot([{ symbol: 'A.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 0: '+', 287: '+' }) }]);

  it('adds new symbols and updates cells and flips', () => {
    const s = applyDelta(base(), {
      spanEnd: w(287),
      cells: [['A.FR', w(287), '-', 'E', 'FR', w(287)], ['B.NL', w(286), '+', 'I', 'NL', w(286), 5, 2, 1]],
      points: null,
      clock,
    });
    expect(cellAt(s, 0, 287)).toBe(DOWN);
    expect(s.rows[0].lastFlip).toBe(w(287));
    expect(s.rows[1]).toEqual({ symbol: 'B.NL', secType: 'I', exchange: 'NL', lastFlip: w(286), last: [w(286), 5, 2, 1] });
    expect(cellAt(s, 1, 286)).toBe(UP);
  });

  it('keeps each row\'s newest window, ignoring a replaced older one', () => {
    let s = snapshot([{ symbol: 'A.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 287: '+' }), last: [w(287), 10, 2, 1] }]);
    expect(s.rows[0].last).toEqual([w(287), 10, 2, 1]);
    s = applyDelta(s, { spanEnd: w(287), cells: [['A.FR', w(286), '+', 'E', 'FR', null, 9, 2, 1]], points: null, clock });
    expect(s.rows[0].last).toEqual([w(287), 10, 2, 1]);
    s = applyDelta(s, { spanEnd: w(288), cells: [['A.FR', w(288), '-', 'E', 'FR', w(288), 11, 1, 2]], points: null, clock });
    expect(s.rows[0].last).toEqual([w(288), 11, 1, 2]);
  });

  it('drops a row\'s newest window once it leaves the span', () => {
    let s = snapshot([{ symbol: 'A.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 287: '+' }), last: [w(287), 10, 2, 1] }]);
    s = applyDelta(s, { spanEnd: w(287 + 287), cells: [], points: null, clock });
    expect(s.rows[0].last).toEqual([w(287), 10, 2, 1]);
    s = applyDelta(s, { spanEnd: w(287 + 288), cells: [], points: null, clock });
    expect(s.rows[0].last).toBeNull();
  });

  it('keeps the sign version on a clock-only delta', () => {
    const s = base();
    expect(applyDelta(s, { spanEnd: w(287), cells: [], points: null, clock }).version).toBe(s.version);
  });

  it('clears the slots that leave the span when it advances', () => {
    const s = applyDelta(base(), { spanEnd: w(288), cells: [['A.FR', w(288), '-', 'E', 'FR', w(288)]], points: null, clock });
    expect(cellAt(s, 0, 287)).toBe(DOWN);
    expect(cellAt(s, 0, 286)).toBe(UP);
    expect(cellAt(s, 0, 0)).toBe(NONE);
  });

  it('clears everything after a jump longer than the span', () => {
    const s = applyDelta(base(), { spanEnd: w(1000), cells: [], points: null, clock });
    for (let c = 0; c < 288; c++) expect(cellAt(s, 0, c)).toBe(NONE);
  });

  it('grows the sign buffer past its initial capacity', () => {
    let s = base();
    const many = Array.from({ length: 40 }, (_, i) => [`S${i}.FR`, w(287), '+', 'E', 'FR', w(287)]);
    s = applyDelta(s, { spanEnd: w(287), cells: many, points: null, clock });
    expect(s.rows.length).toBe(41);
    expect(cellAt(s, 0, 0)).toBe(UP);
    expect(cellAt(s, 40, 287)).toBe(UP);
  });

  it('merges points only into the matching series and trims them to the span', () => {
    let s = applySeries(base(), { series: { symbol: 'A.FR', firstWindowEnd: w(0), points: [[w(0), 1, 1, 1], [w(287), 2, 2, 2]] } });
    s = applyDelta(s, { spanEnd: w(288), cells: [], points: { symbol: 'A.FR', points: [[w(288), 3, 3, 3]] }, clock });
    expect(s.series.points.map((p) => p[0])).toEqual([w(287), w(288)]);

    const other = applyDelta(s, { spanEnd: w(288), cells: [], points: { symbol: 'B.NL', points: [[w(288), 9, 9, 9]] }, clock });
    expect(other.series.points.map((p) => p[1])).toEqual([2, 3]);
  });
});

describe('helpers', () => {
  it('replaces points with the same window and keeps them sorted', () => {
    expect(mergePoints([[2, 1], [1, 1]], [[2, 5], [0, 0]])).toEqual([[0, 0], [1, 1], [2, 5]]);
  });

  it('sorts rows by newest flip, flips before none, then by symbol, and filters', () => {
    const rows = [
      { symbol: 'C.FR', exchange: 'FR', secType: 'E', lastFlip: null },
      { symbol: 'B.NL', exchange: 'NL', secType: 'E', lastFlip: w(1) },
      { symbol: 'A.FR', exchange: 'FR', secType: 'I', lastFlip: w(2) },
      { symbol: 'D.FR', exchange: 'FR', secType: 'E', lastFlip: w(1) },
    ];
    expect(visibleRows(rows, {}).map((i) => rows[i].symbol)).toEqual(['A.FR', 'B.NL', 'D.FR', 'C.FR']);
    expect(visibleRows(rows, { exchange: 'FR', secType: 'E' }).map((i) => rows[i].symbol)).toEqual(['D.FR', 'C.FR']);
  });

  it('gives the share above EMA100 per column, counting only symbols with a window there', () => {
    const s = snapshot([
      { symbol: 'A.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 0: '+', 287: '+' }) },
      { symbol: 'B.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 287: '-' }) },
      { symbol: 'C.FR', secType: 'E', exchange: 'FR', lastFlip: null, cells: cells({ 287: '0' }) },
    ]);
    const share = breadth(s, [0, 1, 2]);
    expect(share[0]).toBe(1);
    expect(share[1]).toBeNull();
    expect(share[287]).toBeCloseTo(1 / 3);
    expect(breadth(s, [1])[287]).toBe(0);
  });

  it('measures the EMA spread against EMA100, so it changes sign at a crossover', () => {
    expect(spreadPct(101, 100)).toBeCloseTo(1);
    expect(spreadPct(99, 100)).toBeCloseTo(-1);
    expect(spreadPct(1, 0)).toBeNull();
    expect([trendOf(0.1), trendOf(-0.1), trendOf(0), trendOf(null)]).toEqual([UP, DOWN, EQUAL, NONE]);
  });

  it('picks quantiles from an ascending array', () => {
    expect(quantile([1, 2, 3, 4], 0.5)).toBe(3);
    expect(quantile([1, 2, 3, 4], 0.95)).toBe(4);
    expect(quantile([], 0.5)).toBeNull();
  });

  it('breaks chart lines across windows without trades', () => {
    const [xs, close] = toChartData([[w(0), 1, 1, 1], [w(1), 2, 2, 2], [w(5), 3, 3, 3]], SLOT);
    expect(xs).toEqual([w(0) / 1000, w(1) / 1000, w(2) / 1000, w(5) / 1000]);
    expect(close).toEqual([1, 2, null, 3]);
  });
});

describe('advisories', () => {
  const adv = (symbol, i, type, exchange = 'FR', secType = 'E') => [symbol, w(i), type, secType, exchange, 2, 1];
  const withAdvisories = (advisories, traders = {}) => applySnapshot(initialState, {
    type: 'snapshot', spanEnd: w(287), slots: 288, slotMs: SLOT, symbols: [], series: null, advisories, traders, clock,
  });

  it('takes every advisory and the traders from a snapshot', () => {
    const s = withAdvisories([adv('A.FR', 10, 'BUY'), adv('B.NL', 12, 'SELL', 'NL')], { t1: ['A.FR'] });
    expect(s.advisories.size).toBe(2);
    expect(s.traders).toEqual({ t1: ['A.FR'] });
  });

  it('merges new advisories by symbol and window, and trims them with the span', () => {
    let s = withAdvisories([adv('A.FR', 0, 'BUY'), adv('A.FR', 10, 'SELL')]);
    s = applyDelta(s, { spanEnd: w(287), cells: [], points: null, advisories: [adv('A.FR', 10, 'SELL'), adv('A.FR', 20, 'BUY')], traders: null, clock });
    expect(s.advisories.size).toBe(3);
    s = applyDelta(s, { spanEnd: w(288), cells: [], points: null, advisories: [], traders: { t2: ['*'] }, clock });
    expect([...s.advisories.values()].map((a) => a.windowEnd)).toEqual([w(10), w(20)]);
    expect(s.traders).toEqual({ t2: ['*'] });
  });

  it('keeps the traders when a delta carries none', () => {
    const s = applyDelta(withAdvisories([], { t1: [] }), { spanEnd: w(287), cells: [], points: null, advisories: [], traders: null, clock });
    expect(s.traders).toEqual({ t1: [] });
  });

  it('indexes newest first, by symbol oldest first, and each symbol latest', () => {
    const s = withAdvisories([adv('B.NL', 5, 'BUY', 'NL'), adv('A.FR', 5, 'BUY'), adv('A.FR', 9, 'SELL')]);
    const { all, bySymbol, lastCross } = indexAdvisories(s.advisories);
    expect(all.map((a) => `${a.symbol}@${a.windowEnd}`)).toEqual([`A.FR@${w(9)}`, `A.FR@${w(5)}`, `B.NL@${w(5)}`]);
    expect(bySymbol.get('A.FR').map((a) => a.type)).toEqual(['BUY', 'SELL']);
    expect(lastCross.get('A.FR').type).toBe('SELL');
  });

  it('filters the feed by exchange, SecType and a trader\'s current list', () => {
    const s = withAdvisories([adv('A.FR', 1, 'BUY'), adv('B.NL', 2, 'SELL', 'NL'), adv('C.NL', 3, 'BUY', 'NL', 'I')]);
    const { all } = indexAdvisories(s.advisories);
    const traders = { t1: ['B.NL', 'C.NL'], t2: ['*'], t3: [] };
    const symbols = (filters) => feedItems(all, filters, traders).map((a) => a.symbol);
    expect(symbols({})).toEqual(['C.NL', 'B.NL', 'A.FR']);
    expect(symbols({ exchange: 'NL', secType: 'E' })).toEqual(['B.NL']);
    expect(symbols({ trader: 't1' })).toEqual(['C.NL', 'B.NL']);
    expect(symbols({ trader: 't2' })).toEqual(['C.NL', 'B.NL', 'A.FR']);
    expect(symbols({ trader: 't3' })).toEqual([]);
    expect(symbols({ trader: 'gone' })).toEqual([]);
    expect(traderScope(traders, null)).toBeNull();
  });

  it('sorts heatmap rows by latest crossover, with none last, and by subscription', () => {
    const rows = [
      { symbol: 'A.FR', exchange: 'FR', secType: 'E', lastFlip: w(9) },
      { symbol: 'B.FR', exchange: 'FR', secType: 'E', lastFlip: w(1) },
      { symbol: 'C.FR', exchange: 'FR', secType: 'E', lastFlip: null },
    ];
    const lastCross = new Map([['B.FR', { windowEnd: w(5) }], ['C.FR', { windowEnd: w(2) }]]);
    const names = (opts, filters = {}) => visibleRows(rows, filters, opts).map((i) => rows[i].symbol);
    expect(names({ sort: 'cross', lastCross })).toEqual(['B.FR', 'C.FR', 'A.FR']);
    expect(names({ sort: 'flip' })).toEqual(['A.FR', 'B.FR', 'C.FR']);
    expect(names({ sort: 'cross', lastCross, traders: { t: ['C.FR', 'A.FR'] } }, { trader: 't' })).toEqual(['C.FR', 'A.FR']);
  });
});

describe('first-window BUYs', () => {
  const close = 19.28;
  const first = ['RDSA.NL', w(0), 'BUY', 'E', 'NL', close * (2 / 39), close * (2 / 101), 1000, 1640];
  const later = ['RDSA.NL', w(9), 'SELL', 'E', 'NL', 19.1, 19.2, 5000, 5300];
  const s = applySnapshot(initialState, {
    type: 'snapshot', spanEnd: w(287), slots: 288, slotMs: SLOT, symbols: [], series: null,
    advisories: [first, later, ['B.NL', w(3), 'BUY', 'E', 'NL', 0.25 * (2 / 39), 0.25 * (2 / 101)]], traders: {}, clock,
  });

  it('are recognised from the EMA ratio alone, for any close', () => {
    expect(isFirstWindowBuy('BUY', 7 * (2 / 39), 7 * (2 / 101))).toBe(true);
    expect(isFirstWindowBuy('SELL', 7 * (2 / 39), 7 * (2 / 101))).toBe(false);
    expect(isFirstWindowBuy('BUY', 19.2712, 19.2655)).toBe(false);
  });

  it('carry their latency from admission to publication', () => {
    const a = s.advisories.get(`RDSA.NL|${w(0)}`);
    expect(a.firstWindow).toBe(true);
    expect(a.latencyMs).toBe(640);
    expect(s.advisories.get(`B.NL|${w(3)}`).latencyMs).toBeNull();
  });

  it('are left out of the feed and the crossover sort unless shown', () => {
    const hidden = indexAdvisories(s.advisories);
    expect(feedItems(hidden.all, {}, {}).map((a) => a.type)).toEqual(['SELL']);
    expect(hidden.lastCross.has('B.NL')).toBe(false);
    expect(hidden.bySymbol.get('B.NL').length).toBe(1);
    const shown = indexAdvisories(s.advisories, { firstWindows: true });
    expect(feedItems(shown.all, { firstWindows: true }, {}).length).toBe(3);
    expect(shown.lastCross.get('B.NL').windowEnd).toBe(w(3));
  });
});
