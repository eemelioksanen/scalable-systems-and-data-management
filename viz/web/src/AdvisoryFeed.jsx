import { formatDateTime, formatLatency } from './format.js';

// Rendering more than this many rows costs more than it tells; the count says what is left out.
export const FEED_ROWS = 400;

const fmt = (v) => (v == null ? '–' : v.toFixed(4));

export default function AdvisoryFeed({ items, total, hiddenFirst = 0, selected, onSelect, zoneOffset }) {
  const shown = items.slice(0, FEED_ROWS);
  return (
    <section className="panel feed">
      <div className="ptitle">
        <b>Advisory feed</b>
        <span className="m">
          {items.length.toLocaleString('en')} of {total.toLocaleString('en')} advisories in the span
          {hiddenFirst > 0 && <> · {hiddenFirst.toLocaleString('en')} first-window BUYs hidden</>} · newest first · click a row to chart it
        </span>
      </div>
      <div className="feed-scroll">
        <table className="feed-table">
          <thead>
            <tr><th>Window end</th><th>Signal</th><th>Symbol</th><th>Type</th><th>Exch.</th><th className="num">EMA38</th><th className="num">EMA100</th>
              <th className="num" title="Wall-clock time from admitting the window's last trade to publishing the advisory (D-40)">Latency</th></tr>
          </thead>
          <tbody>
            {shown.map((a) => (
              <tr key={`${a.symbol}|${a.windowEnd}`} className={a.symbol === selected ? 'sel' : undefined} onClick={() => onSelect(a.symbol)}>
                <td>{formatDateTime(a.windowEnd, zoneOffset).slice(0, -3)}</td>
                <td>
                  <span className={a.type === 'BUY' ? 'sig buy' : 'sig sell'}>{a.type === 'BUY' ? '▲ BUY' : '▼ SELL'}</span>
                  {a.firstWindow && <span className="first-tag" title="From EMA0 = 0 (D-7), not a breakout">first window</span>}
                </td>
                <td>{a.symbol}</td>
                <td>{a.secType === 'I' ? 'index' : 'equity'}</td>
                <td>{a.exchange}</td>
                <td className="num">{fmt(a.ema38)}</td>
                <td className="num">{fmt(a.ema100)}</td>
                <td className="num">{formatLatency(a.latencyMs)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {items.length === 0 && <div className="chart-empty">No advisories match the filters in the last 24 h.</div>}
        {items.length > FEED_ROWS && <div className="note">Showing the newest {FEED_ROWS}; narrow the filters to see older ones.</div>}
      </div>
      <div className="note">
        Read from <code>advisories</code> as the job published them; nothing is recomputed. Times are <code>windowEnd</code> at UTC{zoneOffset}; latency is wall-clock, so it does not depend on the replay rate.
        A subscription filter uses the trader&apos;s current list, so it also shows advisories published before the trader subscribed.
      </div>
    </section>
  );
}
