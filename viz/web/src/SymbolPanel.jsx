import { useMemo, useRef, useState } from 'react';
import Plot from './Plot.jsx';
import { C, breadthOptions, priceOptions, spreadOptions } from './charts.js';
import { spreadPct, toChartData, trendOf } from './state.js';
import { TREND, formatPct, formatPrice, formatTime } from './format.js';

const NO_POINTS = [];

export default function SymbolPanel({ state, series, row, markers, share, zoneOffset }) {
  const [hover, setHover] = useState(null);
  const markersRef = useRef(markers);
  markersRef.current = markers;
  const firstRef = useRef(null);
  const spanRef = useRef(null);
  spanRef.current = state.spanEnd == null ? null : [(state.spanEnd - (state.slots - 1) * state.slotMs) / 1000, state.spanEnd / 1000];
  const getSpan = () => spanRef.current;
  // Marked only where a published BUY has the first window's EMAs: a symbol subscribed mid-run is
  // first published at some later window, which is not its first (D-41).
  firstRef.current = markers.find((m) => m.firstWindow)?.windowEnd ?? null;

  const points = series?.points ?? NO_POINTS;
  const data = useMemo(() => toChartData(points, state.slotMs), [points, state.slotMs]);
  const priceData = useMemo(() => data.slice(0, 4), [data]);
  const spreadData = useMemo(() => [data[0], data[4]], [data]);
  const priceOpts = useMemo(() => priceOptions({
    zone: zoneOffset, format: formatPrice, getSpan, getMarkers: () => markersRef.current, getFirst: () => firstRef.current, onCursor: setHover,
  }), [zoneOffset]);
  const spreadOpts = useMemo(() => spreadOptions({ zone: zoneOffset, format: formatPct, getSpan }), [zoneOffset]);
  const breadthOpts = useMemo(() => breadthOptions({ zone: zoneOffset, getSpan }), [zoneOffset]);
  const breadthData = useMemo(() => {
    const xs = [];
    for (let c = 0; c < state.slots; c++) xs.push(((state.spanEnd ?? 0) - (state.slots - 1 - c) * state.slotMs) / 1000);
    return [xs, share.map((v) => (v == null ? null : v * 100))];
  }, [share, state.spanEnd, state.slots, state.slotMs]);

  // The span can move without a new point for this symbol; the shared x range must follow it.
  const redraw = useMemo(() => ({}), [markers, state.spanEnd]);
  const h = hover != null && data[1][hover] != null ? hover : null;
  const last = points.at(-1);
  const shown = h != null
    ? { t: data[0][h] * 1000, close: data[1][h], ema38: data[2][h], ema100: data[3][h] }
    : last && { t: last[0], close: last[1], ema38: last[2], ema100: last[3] };
  const spread = shown ? spreadPct(shown.ema38, shown.ema100) : null;
  const trend = TREND[trendOf(spread)];
  const buys = markers.filter((m) => m.type === 'BUY' && !m.firstWindow).length;
  const sells = markers.filter((m) => m.type === 'SELL').length;
  const symbol = series?.symbol ?? row?.symbol;

  let empty = null;
  if (!symbol) empty = 'Select a symbol in the watchlist.';
  else if (series && points.length === 0) empty = 'No windows for this symbol in the last 24 h.';

  return (
    <main className="symbol">
      <div className="quote">
        <div>
          <div className="quote-sym">
            {symbol ?? '–'}
            {row && <small>{row.exchange} · {row.secType === 'I' ? 'index' : 'equity'} · {points.length} of {state.slots} windows · {buys} BUY · {sells} SELL</small>}
          </div>
          <div className="quote-px">
            <b>{formatPrice(shown?.close)}</b>
            {trend && <span className={trend.cls}>{trend.arrow} {formatPct(spread)}</span>}
          </div>
        </div>
        <dl className="quote-stats">
          <div><dt><i style={{ background: C.ema38 }} />EMA38</dt><dd>{formatPrice(shown?.ema38)}</dd></div>
          <div><dt><i className="dash" />EMA100</dt><dd>{formatPrice(shown?.ema100)}</dd></div>
          <div><dt>{h != null ? 'HOVERED WINDOW' : 'LATEST WINDOW'}</dt><dd>{shown ? formatTime(shown.t, zoneOffset) : '–'}</dd></div>
        </dl>
      </div>
      <div className="pane pane-price">
        <Plot options={priceOpts} data={priceData} redraw={redraw} className="plot" />
        {empty && <div className="pane-empty">{empty}</div>}
      </div>
      <div className="pane-label">EMA38 − EMA100 <span>as % of EMA100 · a crossover is a change of sign; a first-window BUY needs none</span></div>
      <div className="pane pane-spread"><Plot options={spreadOpts} data={spreadData} redraw={redraw} className="plot" /></div>
      <div className="pane-label">MARKET BREADTH <span>share of the listed symbols with EMA38 above EMA100, per window</span></div>
      <div className="pane pane-breadth"><Plot options={breadthOpts} data={breadthData} className="plot" /></div>
    </main>
  );
}
