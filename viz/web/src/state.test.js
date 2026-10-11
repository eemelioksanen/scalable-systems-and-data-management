import { describe, expect, it } from 'vitest';
import {
  DOWN, EQUAL, NONE, UP, applyDelta, applySeries, applySnapshot, cellAt, initialState,
  mergePoints, toChartData, visibleRows,
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
      cells: [['A.FR', w(287), '-', 'E', 'FR', w(287)], ['B.NL', w(286), '+', 'I', 'NL', w(286)]],
      points: null,
      clock,
    });
    expect(cellAt(s, 0, 287)).toBe(DOWN);
    expect(s.rows[0].lastFlip).toBe(w(287));
    expect(s.rows[1]).toEqual({ symbol: 'B.NL', secType: 'I', exchange: 'NL', lastFlip: w(286) });
    expect(cellAt(s, 1, 286)).toBe(UP);
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

  it('breaks chart lines across windows without trades', () => {
    const [xs, close] = toChartData([[w(0), 1, 1, 1], [w(1), 2, 2, 2], [w(5), 3, 3, 3]], SLOT);
    expect(xs).toEqual([w(0) / 1000, w(1) / 1000, w(2) / 1000, w(5) / 1000]);
    expect(close).toEqual([1, 2, null, 3]);
  });
});
