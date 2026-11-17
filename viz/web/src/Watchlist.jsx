import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { C } from './charts.js';
import { DOWN, EQUAL, UP, cellAt, rowSpread, trendOf } from './state.js';
import { TREND, formatPct, formatPrice, formatTime } from './format.js';

const ROW_H = 46;
const OVERSCAN = 6;
const STRIP_W = 144;
const STRIP_H = 14;
const EXCHANGES = [null, 'FR', 'NL', 'ETR'];
const SEC_TYPES = [null, 'E', 'I'];
const SORTS = ['cross', 'flip', 'symbol'];
const SORT_LABELS = { cross: 'CROSS', flip: 'FLIP', symbol: 'A–Z' };
const SORT_TITLES = {
  cross: 'Newest published advisory first',
  flip: 'Newest sign change of EMA38 − EMA100 first',
  symbol: 'Alphabetical',
};
const CELL = { [UP]: 'rgba(46, 196, 160, 0.45)', [DOWN]: 'rgba(240, 97, 109, 0.45)', [EQUAL]: C.equal };
const EMPTY = 'rgba(136, 145, 161, 0.10)';

function Seg({ values, value, onChange, label = (v) => v ?? 'ALL', title }) {
  return (
    <span className="seg">
      {values.map((v) => (
        <button key={v ?? 'all'} type="button" className={v === value ? 'on' : undefined} title={title?.(v)} onClick={() => onChange(v)}>
          {label(v)}
        </button>
      ))}
    </span>
  );
}

function describeList(list) {
  if (list.includes('*')) return 'all symbols';
  return `${list.length} symbol${list.length === 1 ? '' : 's'}`;
}

// STRIP_W is half the slots, so at 2× pixel density each window gets one device pixel.
function TrendStrip({ state, r, ticks }) {
  const ref = useRef(null);
  useEffect(() => {
    const canvas = ref.current;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(STRIP_W * dpr);
    canvas.height = Math.round(STRIP_H * dpr);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, STRIP_W, STRIP_H);
    const w = STRIP_W / state.slots;
    for (let c = 0; c < state.slots; c++) {
      ctx.fillStyle = CELL[cellAt(state, r, c)] ?? EMPTY;
      ctx.fillRect(c * w, 0, w, STRIP_H);
    }
    for (const a of ticks) {
      const c = state.slots - 1 - Math.round((state.spanEnd - a.windowEnd) / state.slotMs);
      if (c < 0 || c >= state.slots) continue;
      ctx.fillStyle = a.type === 'BUY' ? C.up : C.down;
      ctx.fillRect(c * w - 0.5, 0, Math.max(1.5, w), STRIP_H);
    }
  }, [state, r, ticks]);
  return <canvas ref={ref} className="strip" style={{ width: STRIP_W, height: STRIP_H }} />;
}

export default function Watchlist({ state, order, sort, onSort, filters, onFilters, adv, showFirst, selected, onSelect, zoneOffset }) {
  const scrollRef = useRef(null);
  const [view, setView] = useState({ top: 0, height: 0 });
  const names = Object.keys(state.traders).sort();
  const missing = filters.trader && !state.traders[filters.trader];

  useLayoutEffect(() => {
    const el = scrollRef.current;
    const measure = () => setView({ top: el.scrollTop, height: el.clientHeight });
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    el.addEventListener('scroll', measure, { passive: true });
    return () => {
      ro.disconnect();
      el.removeEventListener('scroll', measure);
    };
  }, []);

  useEffect(() => {
    const el = scrollRef.current;
    const i = order.findIndex((r) => state.rows[r].symbol === selected);
    if (i < 0) return;
    if (i * ROW_H < el.scrollTop || (i + 1) * ROW_H > el.scrollTop + el.clientHeight) el.scrollTop = i * ROW_H - el.clientHeight / 3;
    // Only on a new selection, not on every reorder.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected]);

  const first = Math.max(0, Math.floor(view.top / ROW_H) - OVERSCAN);
  const last = Math.min(order.length, Math.ceil((view.top + view.height) / ROW_H) + OVERSCAN);
  const rows = [];
  for (let i = first; i < last; i++) {
    const r = order[i];
    const row = state.rows[r];
    const ticks = (adv.bySymbol.get(row.symbol) ?? []).filter((a) => showFirst || !a.firstWindow);
    const spread = rowSpread(row);
    const trend = TREND[trendOf(spread)];
    const cross = adv.lastCross.get(row.symbol);
    const dot = row.symbol.lastIndexOf('.');
    let mark = null;
    if (sort === 'flip') {
      mark = row.lastFlip == null ? null : <span title="Latest sign change of EMA38 − EMA100">⇅ {formatTime(row.lastFlip, zoneOffset)}</span>;
    } else if (cross) {
      mark = <span className={cross.type === 'BUY' ? 'up' : 'dn'} title={`Latest ${cross.type}`}>{cross.type === 'BUY' ? '▲' : '▼'} {formatTime(cross.windowEnd, zoneOffset)}</span>;
    }
    rows.push(
      <button key={row.symbol} type="button" className={row.symbol === selected ? 'wl-row sel' : 'wl-row'}
        style={{ top: i * ROW_H }} onClick={() => onSelect(row.symbol)}>
        <span className="wl-sym">
          <b>{dot > 0 ? row.symbol.slice(0, dot) : row.symbol}</b>
          <small>{row.exchange} · {row.secType === 'I' ? 'IDX' : 'EQ'} {mark}</small>
        </span>
        <TrendStrip state={state} r={r} ticks={ticks} />
        <span className="num">{formatPrice(row.last?.[1])}</span>
        <span className={`num wl-spread ${trend?.cls ?? ''}`}>
          {formatPct(spread)}
          {trend && <i>{trend.word}</i>}
        </span>
      </button>,
    );
  }

  return (
    <aside className="watch">
      <div className="panel-head">
        <span>WATCHLIST</span>
        <span className="dim">{order.length.toLocaleString('en')} of {state.rows.length.toLocaleString('en')}</span>
      </div>
      <div className="watch-filters">
        <Seg values={EXCHANGES} value={filters.exchange ?? null} onChange={(exchange) => onFilters({ ...filters, exchange })} />
        <Seg values={SEC_TYPES} value={filters.secType ?? null} label={(v) => (v == null ? 'ALL' : v === 'E' ? 'EQ' : 'IDX')}
          onChange={(secType) => onFilters({ ...filters, secType })} />
        <select value={filters.trader ?? ''} title="A trader's current list in the subscriptions topic (D-17)"
          onChange={(e) => onFilters({ ...filters, trader: e.target.value || null })}>
          <option value="">All published symbols</option>
          {missing && <option value={filters.trader}>{filters.trader} (removed)</option>}
          {names.map((t) => <option key={t} value={t}>{t} · {describeList(state.traders[t])}</option>)}
        </select>
        <span className="watch-sort">SORT <Seg values={SORTS} value={sort} label={(v) => SORT_LABELS[v]} title={(v) => SORT_TITLES[v]} onChange={onSort} /></span>
      </div>
      <div className="wl-head">
        <span>SYMBOL</span>
        <span title="The last 24 h of 5-minute windows: green where EMA38 is above EMA100, red below; solid marks are published advisories">TREND 24H</span>
        <span>CLOSE</span>
        <span title="EMA38 − EMA100 as a percentage of EMA100, at the newest window">SPREAD</span>
      </div>
      <div className="wl-scroll" ref={scrollRef}>
        <div style={{ height: order.length * ROW_H, position: 'relative' }}>{rows}</div>
        {order.length === 0 && <div className="empty">No symbols match the filters.</div>}
      </div>
    </aside>
  );
}
