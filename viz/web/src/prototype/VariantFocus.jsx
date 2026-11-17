// PROTOTYPE variant C: one symbol at a time, light, broker-app style. The page scrolls: focus
// chart, then what the signals did, then the whole market's trend history. Palette "Sheet and
// biro" and the Instrument Sans pairing from taste-skill's fintech library.
import { useMemo, useState } from 'react';
import './focus.css';
import { DOWN, UP, cellAt } from '../state.js';
import { formatDateTime, formatLatency, formatTime, clockStatus } from '../format.js';
import {
  HourBars, LatencyStrip, PriceChart, Sparkline, SpreadChart, change, chartData, latencyStats, pct, price, spreadPct, useFonts, useWidth,
} from './lib.jsx';

const THEME = {
  bg: '#ffffff', ink: '#111111', axis: '#8a8a8a', grid: 'rgba(17,17,17,0.06)', selRow: 'rgba(26,75,224,0.06)',
  close: '#111111', ema38: '#1a4be0', ema100: '#b9b9b9', dash100: [5, 4],
  areaTop: 'rgba(26,75,224,0.14)', areaBottom: 'rgba(26,75,224,0)',
  buy: '#0a8f5a', sell: '#d6334a', buyFill: 'rgba(10,143,90,0.16)', sellFill: 'rgba(214,51,74,0.16)',
  axisFont: '11px "DM Mono", ui-monospace, monospace',
};
const RANGES = [[2, '2H'], [6, '6H'], [12, '12H'], [null, '24H']];

function TrendHistory({ state, order, adv, selected, onSelect, zone }) {
  const [ref, { w }] = useWidth();
  const labelW = 110;
  const rowH = 20;
  const cw = Math.max(0, w - labelW) / state.slots;
  const start = state.spanEnd - (state.slots - 1) * state.slotMs;
  return (
    <div ref={ref} className="f-history">
      {w > 0 && (
        <svg width={w} height={order.length * rowH + 20}>
          {order.map((r, i) => {
            const rw = state.rows[r];
            const cells = [];
            for (let c = 0; c < state.slots; c++) {
              const v = cellAt(state, r, c);
              if (v === UP || v === DOWN) cells.push(<rect key={c} x={labelW + c * cw} y={i * rowH + 3} width={cw + 0.3} height={rowH - 6} fill={v === UP ? THEME.buyFill : THEME.sellFill} />);
            }
            return (
              <g key={rw.symbol} onClick={() => onSelect(rw.symbol)} style={{ cursor: 'pointer' }}>
                <rect x={0} y={i * rowH} width={w} height={rowH} fill={rw.symbol === selected ? THEME.selRow : 'transparent'} />
                <text x={0} y={i * rowH + 14} fontSize="12" fill={rw.symbol === selected ? THEME.ema38 : THEME.ink} fontWeight={rw.symbol === selected ? 600 : 400}>{rw.symbol}</text>
                {cells}
                {(adv.bySymbol.get(rw.symbol) ?? []).filter((a) => !a.firstWindow).map((a) => {
                  const c = state.slots - 1 - Math.round((state.spanEnd - a.windowEnd) / state.slotMs);
                  return <rect key={a.windowEnd} x={labelW + c * cw - 0.5} y={i * rowH + 1} width={Math.max(2, cw)} height={rowH - 2} fill={a.type === 'BUY' ? THEME.buy : THEME.sell} />;
                })}
              </g>
            );
          })}
          {[0, 0.25, 0.5, 0.75, 1].map((f) => (
            <text key={f} x={labelW + f * (w - labelW)} y={order.length * rowH + 15} fontSize="11" fill={THEME.axis}
              textAnchor={f === 0 ? 'start' : f === 1 ? 'end' : 'middle'}>{formatTime(start + f * (state.slots - 1) * state.slotMs, zone)}</text>
          ))}
        </svg>
      )}
    </div>
  );
}

export default function Focus({ state, status, quotes, adv, order, feed, filters, setFilters, selected, select, series, row, zone }) {
  useFonts('https://fonts.googleapis.com/css2?family=Instrument+Sans:wdth,wght@75..100,400..700&family=DM+Mono:wght@400;500&display=swap');
  const [range, setRange] = useState(null);
  const [hover, setHover] = useState(null);
  const points = series?.points ?? [];
  const d = useMemo(() => chartData(points, state.slotMs, range), [points, state.slotMs, range]);
  const markers = selected ? adv.bySymbol.get(selected) ?? [] : [];
  const q = quotes[selected];
  const sp = spreadPct(q);
  const bull = sp != null && sp >= 0;
  const c = change(q);
  const lat = latencyStats(feed);
  const s = clockStatus(state.clock, Date.now(), state.slotMs, status);
  const h = hover != null && d.xs[hover] != null ? hover : null;
  const big = h != null ? d.close[h] : q?.close;

  return (
    <div className="f-root">
      <nav className="f-nav">
        <span className="f-brand">Trading Trends</span>
        <div className="f-tabs">
          {order.map((r) => {
            const sym = state.rows[r].symbol;
            const qq = quotes[sym];
            const cc = change(qq);
            return (
              <button key={sym} type="button" className={sym === selected ? 'on' : ''} onClick={() => select(sym)}>
                <span>{sym}</span>
                <Sparkline values={qq?.spark?.slice(-24)} width={44} height={16} color={cc >= 0 ? THEME.buy : THEME.sell} />
                <small className={cc >= 0 ? 'up' : 'dn'}>{pct(cc, 1)}</small>
              </button>
            );
          })}
        </div>
        <span className={`f-status ${s.key}`}><i />{s.text}</span>
      </nav>

      <div className="f-filters">
        {[[null, 'All'], ['FR', 'Paris'], ['NL', 'Amsterdam'], ['ETR', 'Xetra']].map(([v, l]) => (
          <button key={l} type="button" className={filters.exchange === v || (!filters.exchange && v == null) ? 'on' : ''} onClick={() => setFilters({ ...filters, exchange: v })}>{l}</button>
        ))}
        <span className="f-div" />
        {[[null, 'Everything'], ['E', 'Equities'], ['I', 'Indices']].map(([v, l]) => (
          <button key={l} type="button" className={filters.secType === v || (!filters.secType && v == null) ? 'on' : ''} onClick={() => setFilters({ ...filters, secType: v })}>{l}</button>
        ))}
        <span className="f-div" />
        <select value={filters.trader ?? ''} onChange={(e) => setFilters({ ...filters, trader: e.target.value || null })}>
          <option value="">Every trader</option>
          {Object.keys(state.traders).sort().map((t) => <option key={t} value={t}>{t}&apos;s list</option>)}
        </select>
      </div>

      <section className="f-hero">
        <div className="f-main">
          <div className="f-name">{selected}<small>{row ? `${row.exchange} · ${row.secType === 'I' ? 'Index' : 'Equity'}` : ''}</small></div>
          <div className="f-price">
            <b>{price(big)}</b>
            <span className={`f-chip ${c >= 0 ? 'up' : 'dn'}`}>{pct(c)}</span>
            {h != null && <span className="f-at">at {formatTime(d.xs[h] * 1000, zone)}</span>}
          </div>
          <p className="f-says">
            EMA38 is <b className={bull ? 'up' : 'dn'}>{pct(Math.abs(sp ?? 0), 3).slice(1)} {bull ? 'above' : 'below'}</b> EMA100
            {row?.lastFlip != null && <> and has been since <b>{formatTime(row.lastFlip, zone)}</b></>}.
            {' '}Results as of {formatDateTime(state.clock?.resultsMs ?? null, zone).slice(4, -3)}.
          </p>
          <div className="f-range">
            {RANGES.map(([v, l]) => <button key={l} type="button" className={range === v ? 'on' : ''} onClick={() => setRange(v)}>{l}</button>)}
            <span className="f-legend">
              <span><i style={{ background: THEME.close }} />Close</span>
              <span><i style={{ background: THEME.ema38 }} />EMA38</span>
              <span><i className="dash" />EMA100</span>
            </span>
          </div>
          <div className="f-pc"><PriceChart d={d} markers={markers} theme={THEME} zone={zone} onCursor={setHover} className="fill" syncKey="f" /></div>
          <div className="f-sc-label">Distance between the EMAs <small>a BUY or SELL is published where it crosses zero</small></div>
          <div className="f-sc"><SpreadChart d={d} theme={THEME} zone={zone} className="fill" syncKey="f" /></div>
        </div>

        <aside className="f-side">
          <h2>Signals</h2>
          <ol className="f-story">
            {feed.slice(0, 8).map((a) => (
              <li key={`${a.symbol}|${a.windowEnd}`} className={a.symbol === selected ? 'sel' : ''}>
                <button type="button" onClick={() => select(a.symbol)}>
                  <span className={`f-dot ${a.type === 'BUY' ? 'up' : 'dn'}`}>{a.type === 'BUY' ? '↗' : '↘'}</span>
                  <span>
                    <b>{a.type === 'BUY' ? 'Buy' : 'Sell'} {a.symbol}</b>
                    <small>EMA38 crossed {a.type === 'BUY' ? 'above' : 'below'} EMA100 · {formatTime(a.windowEnd, zone)} · published in {formatLatency(a.latencyMs)}</small>
                  </span>
                </button>
              </li>
            ))}
            {feed.length === 0 && <li className="dim">No crossovers yet.</li>}
          </ol>
          <h2>Signals per hour</h2>
          <HourBars advisories={feed} state={state} theme={THEME} zone={zone} />
          <h2>Time to advisory <small>{lat ? `p50 ${formatLatency(lat.p50)} · p95 ${formatLatency(lat.p95)}` : ''}</small></h2>
          <LatencyStrip advisories={feed} theme={THEME} />
        </aside>
      </section>

      <section className="f-hist">
        <h2>Trend history <small>last 24 h of 5-minute windows · tinted by which EMA leads · solid marks are published crossovers · click a row</small></h2>
        <TrendHistory state={state} order={order} adv={adv} selected={selected} onSelect={select} zone={zone} />
      </section>
    </div>
  );
}
