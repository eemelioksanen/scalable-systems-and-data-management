// PROTOTYPE variant A: a dark three-column trading terminal. Palette "Night and sky blue" and the
// Geist pairing, dealt from taste-skill's fintech library.
import { useMemo, useState } from 'react';
import './terminal.css';
import { formatDateTime, formatLatency, formatTime, clockStatus } from '../format.js';
import {
  BreadthChart, PriceChart, SignStrip, SpreadChart, breadth, change, chartData, latencyStats, pct, price, spreadPct, useFonts,
} from './lib.jsx';
import { useFresh } from './useFresh.js';

const THEME = {
  bg: '#0a0d14', ink: '#e9edf3', axis: '#5d6678', grid: 'rgba(136,145,161,0.10)', selRow: 'rgba(90,185,252,0.08)',
  close: '#e9edf3', ema38: '#5ab9fc', ema100: '#f5b84a', dash100: [4, 3],
  areaTop: 'rgba(90,185,252,0.16)', areaBottom: 'rgba(90,185,252,0)',
  buy: '#2ec4a0', sell: '#f0616d', buyFill: 'rgba(46,196,160,0.22)', sellFill: 'rgba(240,97,109,0.22)',
  axisFont: '10px "Geist Mono", ui-monospace, monospace',
};

const EXCHANGES = [null, 'FR', 'NL', 'ETR'];
const TYPES = [null, 'E', 'I'];

function Seg({ values, value, onChange, label = (v) => v ?? 'ALL' }) {
  return (
    <span className="t-seg">
      {values.map((v) => (
        <button key={v ?? 'all'} type="button" className={v === value ? 'on' : ''} onClick={() => onChange(v)}>{label(v)}</button>
      ))}
    </span>
  );
}

export default function Terminal({ state, status, quotes, adv, order, feed, filters, setFilters, selected, select, series, row, zone }) {
  useFonts('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap');
  const [hover, setHover] = useState(null);
  const points = series?.points ?? [];
  const d = useMemo(() => chartData(points, state.slotMs), [points, state.slotMs]);
  const b = useMemo(() => breadth(state, order), [state, order]);
  const markers = selected ? adv.bySymbol.get(selected) ?? [] : [];
  const q = quotes[selected];
  const lat = latencyStats(feed);
  const fresh = useFresh(feed.map((a) => `${a.symbol}|${a.windowEnd}`));
  const s = clockStatus(state.clock, Date.now(), state.slotMs, status);
  const h = hover != null && d.xs[hover] != null ? hover : null;
  const shown = h != null
    ? { t: d.xs[h] * 1000, close: d.close[h], e38: d.e38[h], e100: d.e100[h], spread: d.spread[h] }
    : q && { t: q.at, close: q.close, e38: q.ema38, e100: q.ema100, spread: spreadPct(q) };
  const latestBreadth = b.share.findLast((v) => v != null);
  const buys = feed.filter((a) => a.type === 'BUY').length;

  return (
    <div className="t-root">
      <header className="t-top">
        <span className="t-mark"><i />EMA<b>X</b></span>
        <span className="t-kv"><span>RESULTS AS OF</span><b>{formatDateTime(state.clock?.resultsMs ?? null, zone).slice(0, -3)}</b></span>
        <span className={`t-state ${s.key}`}>{s.text}{s.nextEnd != null && ` → ${formatTime(s.nextEnd, zone)}`}</span>
        <span className="t-kv dim"><span>FEED</span>{formatDateTime(state.clock?.feedMs ?? null, zone)}</span>
        <span className="t-sp" />
        <span className="t-kv"><span>BREADTH</span><b className={latestBreadth >= 0.5 ? 'up' : 'dn'}>{latestBreadth == null ? '–' : `${Math.round(latestBreadth * 100)}%`}</b></span>
        <span className="t-kv"><span>SIGNALS</span><b className="up">{buys}▲</b><b className="dn">{feed.length - buys}▼</b></span>
        <span className="t-kv"><span>P50 LATENCY</span><b>{lat ? formatLatency(lat.p50) : '–'}</b></span>
        <span className={status === 'live' ? 't-live on' : 't-live'}>{status === 'live' ? 'LIVE' : status.toUpperCase()}</span>
      </header>

      <div className="t-tape">
        {order.map((r) => {
          const sym = state.rows[r].symbol;
          const c = change(quotes[sym]);
          return (
            <button key={sym} type="button" className={sym === selected ? 'sel' : ''} onClick={() => select(sym)}>
              <b>{sym}</b><span>{price(quotes[sym]?.close)}</span><span className={c >= 0 ? 'up' : 'dn'}>{pct(c)}</span>
            </button>
          );
        })}
      </div>

      <aside className="t-watch">
        <div className="t-ph">
          <span>WATCHLIST</span>
          <span className="dim">{order.length} symbols</span>
        </div>
        <div className="t-filters">
          <Seg values={EXCHANGES} value={filters.exchange ?? null} onChange={(exchange) => setFilters({ ...filters, exchange })} />
          <Seg values={TYPES} value={filters.secType ?? null} label={(v) => (v == null ? 'ALL' : v === 'E' ? 'EQ' : 'IDX')} onChange={(secType) => setFilters({ ...filters, secType })} />
          <select value={filters.trader ?? ''} onChange={(e) => setFilters({ ...filters, trader: e.target.value || null })}>
            <option value="">All traders</option>
            {Object.keys(state.traders).sort().map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div className="t-wl-head"><span>SYMBOL</span><span>TREND 6H</span><span>LAST</span><span>CHG</span></div>
        <div className="t-wl">
          {order.map((r) => {
            const rw = state.rows[r];
            const qq = quotes[rw.symbol];
            const c = change(qq);
            const last = adv.lastCross.get(rw.symbol);
            const bull = qq && qq.ema38 > qq.ema100;
            return (
              <button key={rw.symbol} type="button" className={rw.symbol === selected ? 'sel' : ''} onClick={() => select(rw.symbol)}>
                <span className="sym">
                  <b>{rw.symbol.split('.')[0]}</b>
                  <small>{rw.exchange} · {rw.secType === 'I' ? 'IDX' : 'EQ'}{last && <em className={last.type === 'BUY' ? 'up' : 'dn'}> {last.type === 'BUY' ? '▲' : '▼'}{formatTime(last.windowEnd, zone)}</em>}</small>
                </span>
                <SignStrip state={state} row={r} columns={72} width={84} height={14} up={THEME.buy} down={THEME.sell} empty="rgba(136,145,161,0.12)" />
                <span className="num">{price(qq?.close)}</span>
                <span className={`num chg ${c >= 0 ? 'up' : 'dn'}`}>{pct(c)}<i className={bull ? 'up' : 'dn'}>{bull ? 'BULL' : 'BEAR'}</i></span>
              </button>
            );
          })}
        </div>
      </aside>

      <main className="t-main">
        <div className="t-quote">
          <div>
            <div className="t-sym">{selected ?? '–'} <small>{row ? `${row.exchange} · ${row.secType === 'I' ? 'index' : 'equity'}` : ''}</small></div>
            <div className="t-price">
              <b>{price(shown?.close)}</b>
              <span className={change(q) >= 0 ? 'up' : 'dn'}>{pct(change(q))} <small>span</small></span>
            </div>
          </div>
          <dl className="t-stats">
            <div><dt><i style={{ background: THEME.ema38 }} />EMA38</dt><dd>{price(shown?.e38)}</dd></div>
            <div><dt><i style={{ background: THEME.ema100 }} />EMA100</dt><dd>{price(shown?.e100)}</dd></div>
            <div><dt>SPREAD</dt><dd className={shown?.spread >= 0 ? 'up' : 'dn'}>{pct(shown?.spread, 3)}</dd></div>
            <div><dt>WINDOW END</dt><dd>{shown ? formatTime(shown.t, zone) : '–'}</dd></div>
          </dl>
        </div>
        <div className="t-chart">
          <PriceChart d={d} markers={markers} theme={THEME} zone={zone} onCursor={setHover} className="t-pc" />
        </div>
        <div className="t-pane-label">EMA38 − EMA100 <span className="dim">· crossovers are zero-crossings</span></div>
        <div className="t-spread"><SpreadChart d={d} theme={THEME} zone={zone} className="t-sc" /></div>
        <div className="t-pane-label">MARKET BREADTH <span className="dim">· share of symbols with EMA38 above EMA100, per window</span></div>
        <div className="t-breadth"><BreadthChart b={b} theme={THEME} zone={zone} className="t-bc" /></div>
      </main>

      <aside className="t-blotter">
        <div className="t-ph"><span>SIGNALS</span><span className="dim">{feed.length} in span</span></div>
        <div className="t-bl-head"><span>TIME</span><span>SIDE</span><span>SYMBOL</span><span className="num">EMA38</span><span className="num">LAT</span></div>
        <div className="t-bl">
          {feed.slice(0, 200).map((a) => {
            const k = `${a.symbol}|${a.windowEnd}`;
            return (
              <button key={k} type="button" className={`${a.symbol === selected ? 'sel' : ''} ${fresh.has(k) ? 'fresh' : ''}`} onClick={() => select(a.symbol)}>
                <span className="dim">{formatTime(a.windowEnd, zone)}</span>
                <span className={`side ${a.type === 'BUY' ? 'up' : 'dn'}`}>{a.type}</span>
                <span>{a.symbol}</span>
                <span className="num">{price(a.ema38)}</span>
                <span className="num dim">{formatLatency(a.latencyMs)}</span>
              </button>
            );
          })}
          {feed.length === 0 && <div className="t-empty">No crossovers in the span yet.</div>}
        </div>
        {lat && (
          <div className="t-lat">
            <div><span>P50</span><b>{formatLatency(lat.p50)}</b></div>
            <div><span>P95</span><b>{formatLatency(lat.p95)}</b></div>
            <div><span>MAX</span><b>{formatLatency(lat.max)}</b></div>
          </div>
        )}
      </aside>

      <footer className="t-foot">
        <span>ema-results · advisories</span>
        <span>UTC{zone}</span>
        <span>{state.rows.length} symbols published</span>
        <span className="t-sp" />
        <span>5-min tumbling windows · EMA₃₈ / EMA₁₀₀</span>
      </footer>
    </div>
  );
}
