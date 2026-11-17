import { useEffect, useState } from 'react';
import { TREND, clockStatus, formatDateTime, formatLatency, formatLocal, formatMinute, formatPct, formatPrice, formatTime } from './format.js';
import { rowSpread, trendOf } from './state.js';

const TITLES = {
  live: 'The newest window is published and the next one is still open: it is evaluated once the window after it starts (spec 3.1 (7)).',
  wait: 'No newer window is published yet. The job waits for its watermark (the 1 s bound, or a quiet partition up to the 30 s idleness timeout), or no subscribed symbol traded since. Watermark lag is in Prometheus.',
  idle: 'No new ticks. Windows still open are evaluated when input resumes or the job is drained.',
  off: 'Not connected to results yet.',
};
const TAPE = 40;

export default function TopBar({ clock, status, slotMs, breadthNow, buys, sells, p50 }) {
  const [wall, setWall] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setWall(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const zone = clock?.zoneOffset ?? '+02:00';
  const s = clockStatus(clock, wall, slotMs, status);

  return (
    <header className="top">
      <span className="brand"><i />Trading Trends</span>
      <span className="kv" title="windowEnd of the newest published result: the exclusive end of its 5-minute window">
        <span>RESULTS AS OF</span><b>{formatMinute(clock?.resultsMs ?? null, zone)}</b>
      </span>
      <span className={`state ${s.key}`} title={TITLES[s.key]}>{s.text}{s.nextEnd != null && <> → {formatTime(s.nextEnd, zone)}</>}</span>
      <span className="kv dim" title="Sampled from the tail of ticks once a second: the replay's source time, at the job's zone offset. It moves in steps at fast replay rates.">
        <span>FEED</span>{formatDateTime(clock?.feedMs ?? null, zone)}
      </span>
      <span className="kv dim"><span>WALL</span>{formatLocal(wall)}</span>
      <span className="sp" />
      <span className="kv" title="Share of the listed symbols with EMA38 above EMA100 at the newest window that has any">
        <span>BREADTH</span><b className={breadthNow == null ? '' : breadthNow >= 0.5 ? 'up' : 'dn'}>{breadthNow == null ? '–' : `${Math.round(breadthNow * 100)}%`}</b>
      </span>
      <span className="kv" title="Advisories listed in the signals panel"><span>SIGNALS</span><b className="up">{buys}▲</b><b className="dn">{sells}▼</b></span>
      <span className="kv" title="Median advisory latency, emitTs − maxIngestTs (D-40)"><span>P50</span><b>{p50 == null ? '–' : formatLatency(p50)}</b></span>
      <span className="zone" title="The Flink job's --zone-offset (D-3): feed and window times are shown at this offset">UTC{zone}</span>
      <span className={status === 'live' ? 'conn on' : 'conn'}>{status === 'live' ? 'LIVE' : status.toUpperCase()}</span>
    </header>
  );
}

export function Ticker({ rows, order, selected, onSelect }) {
  return (
    <nav className="tape">
      {order.slice(0, TAPE).map((r) => {
        const row = rows[r];
        const spread = rowSpread(row);
        const t = TREND[trendOf(spread)];
        return (
          <button key={row.symbol} type="button" className={row.symbol === selected ? 'sel' : undefined} onClick={() => onSelect(row.symbol)}>
            <b>{row.symbol}</b>
            <span>{formatPrice(row.last?.[1])}</span>
            {t && <span className={t.cls}>{t.arrow}{formatPct(Math.abs(spread)).slice(1)}</span>}
          </button>
        );
      })}
    </nav>
  );
}
