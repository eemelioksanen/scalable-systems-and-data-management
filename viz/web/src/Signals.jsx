import { FLASH_MS } from './useDashboardSocket.js';
import { advisoryKey, quantile } from './state.js';
import { formatDateTime, formatLatency, formatTime } from './format.js';

// Rendering more than this many rows costs more than it tells; the count says what is left out.
export const FEED_ROWS = 400;

export default function Signals({ items, arrivals, latencies, total, hiddenFirst, showFirst, onShowFirst, selected, onSelect, zoneOffset }) {
  const shown = items.slice(0, FEED_ROWS);
  const now = Date.now();

  return (
    <aside className="signals">
      <div className="panel-head">
        <span>SIGNALS</span>
        <span className="dim">{items.length.toLocaleString('en')} of {total.toLocaleString('en')} in span</span>
      </div>
      <label className="first-toggle" title="Every symbol's first window gives a BUY from EMA0 = 0 (D-7): published, but no breakout">
        <input type="checkbox" checked={showFirst} onChange={(e) => onShowFirst(e.target.checked)} />
        first-window BUYs{!showFirst && hiddenFirst > 0 && <span className="dim"> · {hiddenFirst.toLocaleString('en')} hidden</span>}
      </label>
      <div className="sg-head">
        <span>TIME</span><span>SIDE</span><span>SYMBOL</span><span className="num" title="EMA38, and EMA100 under it, as published">EMA38 / 100</span>
        <span className="num" title="Wall-clock time from admitting the window's last trade to the job emitting the advisory (D-40)">LAT</span>
      </div>
      <div className="sg-scroll">
        {shown.map((a) => {
          const key = advisoryKey(a.symbol, a.windowEnd);
          return (
            <button key={key} type="button" title={formatDateTime(a.windowEnd, zoneOffset)}
              className={`sg-row${a.symbol === selected ? ' sel' : ''}${now - (arrivals.get(key) ?? 0) < FLASH_MS ? ' fresh' : ''}`} onClick={() => onSelect(a.symbol)}>
              <span className="dim">{formatTime(a.windowEnd, zoneOffset)}</span>
              <span className={`side ${a.type === 'BUY' ? 'up' : 'dn'}`}>
                {a.type === 'BUY' ? '▲ BUY' : '▼ SELL'}{a.firstWindow && <small title="From EMA0 = 0 (D-7), not a breakout"> 1st</small>}
              </span>
              <span className="sym">{a.symbol}<small>{a.secType === 'I' ? 'IDX' : 'EQ'}</small></span>
              <span className="num emas"><span>{a.ema38.toFixed(4)}</span><span className="dim">{a.ema100.toFixed(4)}</span></span>
              <span className="num dim">{formatLatency(a.latencyMs)}</span>
            </button>
          );
        })}
        {items.length === 0 && <div className="empty">No advisories match the filters in the last 24 h.</div>}
        {items.length > FEED_ROWS && <div className="empty">Showing the newest {FEED_ROWS}; narrow the filters to see older ones.</div>}
      </div>
      <div className="latency" title="Advisory latency, emitTs − maxIngestTs, over the advisories listed. Wall-clock, so it does not depend on the replay rate (D-40).">
        <div><span>P50</span><b>{latencies.length ? formatLatency(quantile(latencies, 0.5)) : '–'}</b></div>
        <div><span>P95</span><b>{latencies.length ? formatLatency(quantile(latencies, 0.95)) : '–'}</b></div>
        <div><span>MAX</span><b>{latencies.length ? formatLatency(latencies.at(-1)) : '–'}</b></div>
      </div>
    </aside>
  );
}
