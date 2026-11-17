// PROTOTYPE variant B: market overview first. Every symbol is a tile; the selected one opens in a
// docked detail column; a crossover timeline and breadth run along the bottom. Palette "Warm
// black, gold" and the Archivo pairing from taste-skill's fintech library, with the project's
// blue/orange BUY/SELL pair kept for colour-blind safety.
import { useMemo } from 'react';
import './marketmap.css';
import { formatDateTime, formatLatency, formatTime, clockStatus } from '../format.js';
import {
  BreadthChart, CrossoverTimeline, PriceChart, SignStrip, Sparkline, SpreadChart, breadth, change, chartData, latencyStats, pct, price, spreadPct, useFonts,
} from './lib.jsx';
import { useFresh } from './useFresh.js';

const THEME = {
  bg: '#0c0a08', ink: '#f6f1ea', axis: '#7d766d', grid: 'rgba(143,138,132,0.12)', selRow: 'rgba(201,164,90,0.10)',
  close: '#f6f1ea', ema38: '#6aa8ff', ema100: '#ff8a3d', dash100: undefined,
  areaTop: 'rgba(201,164,90,0.20)', areaBottom: 'rgba(201,164,90,0)',
  buy: '#6aa8ff', sell: '#ff8a3d', buyFill: 'rgba(106,168,255,0.22)', sellFill: 'rgba(255,138,61,0.22)',
  axisFont: '10px "Martian Mono", ui-monospace, monospace',
};

function Chips({ values, value, onChange, label = (v) => v ?? 'All' }) {
  return (
    <span className="m-chips">
      {values.map((v) => (
        <button key={v ?? 'all'} type="button" className={v === value ? 'on' : ''} onClick={() => onChange(v)}>{label(v)}</button>
      ))}
    </span>
  );
}

function Tile({ state, r, q, last, selected, onSelect, zone, fresh }) {
  const rw = state.rows[r];
  const c = change(q);
  const sp = spreadPct(q);
  const bull = sp != null && sp >= 0;
  return (
    <button type="button" className={`m-tile ${bull ? 'bull' : 'bear'} ${selected ? 'sel' : ''} ${fresh ? 'fresh' : ''}`} onClick={() => onSelect(rw.symbol)}>
      <div className="m-tile-top">
        <span><b>{rw.symbol}</b><small>{rw.exchange} · {rw.secType === 'I' ? 'Index' : 'Equity'}</small></span>
        <span className={`m-trend ${bull ? 'up' : 'dn'}`}>{bull ? '▲ above' : '▼ below'}</span>
      </div>
      <div className="m-tile-px">
        <b>{price(q?.close)}</b>
        <span className={c >= 0 ? 'up' : 'dn'}>{pct(c)}</span>
      </div>
      <Sparkline values={q?.spark} overlay={q?.spark38} width={196} height={44} color={THEME.ink}
        overlayColor={bull ? THEME.buy : THEME.sell} fill={bull ? 'rgba(106,168,255,0.10)' : 'rgba(255,138,61,0.10)'} />
      <SignStrip state={state} row={r} columns={144} width={196} height={6} up={THEME.buy} down={THEME.sell} empty="rgba(143,138,132,0.15)" />
      <div className="m-tile-foot">
        <span>spread <b className={bull ? 'up' : 'dn'}>{pct(sp, 3)}</b></span>
        <span>{last ? <><b className={last.type === 'BUY' ? 'up' : 'dn'}>{last.type}</b> {formatTime(last.windowEnd, zone)}</> : 'no cross'}</span>
      </div>
    </button>
  );
}

export default function MarketMap({ state, status, quotes, adv, order, feed, filters, setFilters, selected, select, series, row, zone }) {
  useFonts('https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@62..125,400..700&family=Martian+Mono:wght@400;500&display=swap');
  const points = series?.points ?? [];
  const d = useMemo(() => chartData(points, state.slotMs), [points, state.slotMs]);
  const b = useMemo(() => breadth(state, order), [state, order]);
  const markers = selected ? adv.bySymbol.get(selected) ?? [] : [];
  const q = quotes[selected];
  const lat = latencyStats(feed);
  const s = clockStatus(state.clock, Date.now(), state.slotMs, status);
  const latestBreadth = b.share.findLast((v) => v != null);
  const buys = feed.filter((a) => a.type === 'BUY').length;
  const freshCross = useFresh(order.map((r) => `${state.rows[r].symbol}|${adv.lastCross.get(state.rows[r].symbol)?.windowEnd ?? ''}`));

  return (
    <div className="m-root">
      <header className="m-head">
        <div className="m-title">
          <h1>Market pulse</h1>
          <p>
            Results as of <b>{formatDateTime(state.clock?.resultsMs ?? null, zone).slice(4, -3)}</b>
            <span className={`m-state ${s.key}`}>{s.text}</span>
          </p>
        </div>
        <div className="m-kpis">
          <div><span>Breadth</span><b className={latestBreadth >= 0.5 ? 'up' : 'dn'}>{latestBreadth == null ? '–' : `${Math.round(latestBreadth * 100)}%`}</b><small>above EMA100</small></div>
          <div><span>Crossovers</span><b><em className="up">{buys}</em><i>/</i><em className="dn">{feed.length - buys}</em></b><small>buy / sell in 24 h</small></div>
          <div><span>Latency p50</span><b>{lat ? formatLatency(lat.p50) : '–'}</b><small>p95 {lat ? formatLatency(lat.p95) : '–'}</small></div>
          <div><span>Symbols</span><b>{order.length}</b><small>of {state.rows.length} published</small></div>
        </div>
      </header>

      <nav className="m-filters">
        <Chips values={[null, 'FR', 'NL', 'ETR']} value={filters.exchange ?? null} onChange={(exchange) => setFilters({ ...filters, exchange })} />
        <Chips values={[null, 'E', 'I']} value={filters.secType ?? null} label={(v) => (v == null ? 'All types' : v === 'E' ? 'Equities' : 'Indices')} onChange={(secType) => setFilters({ ...filters, secType })} />
        <Chips values={[null, ...Object.keys(state.traders).sort()]} value={filters.trader ?? null} label={(v) => v ?? 'All traders'} onChange={(trader) => setFilters({ ...filters, trader })} />
      </nav>

      <section className="m-grid">
        {order.map((r) => {
          const sym = state.rows[r].symbol;
          const last = adv.lastCross.get(sym);
          return <Tile key={sym} state={state} r={r} q={quotes[sym]} last={last} selected={sym === selected} onSelect={select} zone={zone}
            fresh={freshCross.has(`${sym}|${last?.windowEnd ?? ''}`) && last} />;
        })}
      </section>

      <aside className="m-detail">
        <div className="m-detail-head">
          <div>
            <h2>{selected ?? '–'}</h2>
            <small>{row ? `${row.exchange} · ${row.secType === 'I' ? 'Index' : 'Equity'}` : ''}</small>
          </div>
          <div className="m-detail-px">
            <b>{price(q?.close)}</b>
            <span className={change(q) >= 0 ? 'up' : 'dn'}>{pct(change(q))}</span>
          </div>
        </div>
        <div className="m-legend">
          <span><i style={{ background: THEME.close }} />Close</span>
          <span><i style={{ background: THEME.ema38 }} />EMA38 {price(q?.ema38)}</span>
          <span><i style={{ background: THEME.ema100 }} />EMA100 {price(q?.ema100)}</span>
        </div>
        <div className="m-pc"><PriceChart d={d} markers={markers} theme={THEME} zone={zone} className="fill" syncKey="m" /></div>
        <h3>EMA38 − EMA100</h3>
        <div className="m-sc"><SpreadChart d={d} theme={THEME} zone={zone} className="fill" syncKey="m" /></div>
        <h3>Crossovers</h3>
        <ol className="m-events">
          {[...markers].reverse().map((a) => (
            <li key={a.windowEnd}>
              <span className={`m-ev ${a.type === 'BUY' ? 'up' : 'dn'}`}>{a.type === 'BUY' ? '▲' : '▼'}</span>
              <span><b>{a.type}</b> {formatTime(a.windowEnd, zone)}{a.firstWindow && <small> first window</small>}</span>
              <span className="mono">{price(a.ema38)} / {price(a.ema100)}</span>
              <span className="mono dim">{formatLatency(a.latencyMs)}</span>
            </li>
          ))}
          {markers.length === 0 && <li className="dim">No crossover in the last 24 h.</li>}
        </ol>
      </aside>

      <section className="m-bottom">
        <div className="m-timeline">
          <h3>Crossover timeline <small>every published advisory, last 24 h</small></h3>
          <CrossoverTimeline advisories={feed} state={state} theme={THEME} zone={zone} selected={selected} onSelect={select} />
        </div>
        <div className="m-breadth">
          <h3>Breadth <small>share above EMA100</small></h3>
          <div className="m-bc"><BreadthChart b={b} theme={THEME} zone={zone} className="fill" /></div>
        </div>
      </section>
    </div>
  );
}
