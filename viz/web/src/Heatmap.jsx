import { useEffect, useMemo, useRef, useState } from 'react';
import { DOWN, EQUAL, UP, cellAt, visibleRows } from './state.js';
import { formatTime } from './format.js';

const ROW_H = 13;
const LABEL_W = 92;
const FLIP_W = 48;
const COLORS = { [UP]: '#93b4f5', [DOWN]: '#f3c27f', [EQUAL]: '#9aa3ad' };
const EMPTY = '#f1f2f4';
const EXCHANGES = ['FR', 'NL', 'ETR'];
const SEC_TYPES = ['E', 'I'];

function Chips({ label, values, value, onChange }) {
  return (
    <span className="chips">
      {label}
      <button type="button" className={value == null ? 'chip on' : 'chip'} onClick={() => onChange(null)}>All</button>
      {values.map((v) => (
        <button key={v} type="button" className={value === v ? 'chip on' : 'chip'} onClick={() => onChange(v)}>{v}</button>
      ))}
    </span>
  );
}

export default function Heatmap({ state, selected, onSelect, zoneOffset }) {
  const [filters, setFilters] = useState({ exchange: null, secType: null });
  const scrollRef = useRef(null);
  const canvasRef = useRef(null);
  const [viewport, setViewport] = useState({ top: 0, width: 0, height: 0 });

  const order = useMemo(() => visibleRows(state.rows, filters), [state.rows, filters]);

  useEffect(() => {
    const el = scrollRef.current;
    const measure = () => setViewport({ top: el.scrollTop, width: el.clientWidth, height: el.clientHeight });
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
    const canvas = canvasRef.current;
    const { width, height, top } = viewport;
    if (!canvas || width === 0) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
    ctx.textBaseline = 'middle';

    const gridW = Math.max(0, width - LABEL_W - FLIP_W);
    const cellW = gridW / state.slots;
    const first = Math.floor(top / ROW_H);
    const last = Math.min(order.length, Math.ceil((top + height) / ROW_H));
    for (let i = first; i < last; i++) {
      const r = order[i];
      const row = state.rows[r];
      const y = i * ROW_H - top;
      if (row.symbol === selected) {
        ctx.fillStyle = '#eef2ff';
        ctx.fillRect(0, y, width, ROW_H);
      }
      ctx.fillStyle = '#111418';
      ctx.font = `${row.symbol === selected ? '700 ' : ''}11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`;
      ctx.fillText(row.symbol, 2, y + ROW_H / 2, LABEL_W - 8);
      for (let c = 0; c < state.slots; c++) {
        const v = cellAt(state, r, c);
        ctx.fillStyle = COLORS[v] ?? EMPTY;
        ctx.fillRect(LABEL_W + c * cellW, y + 1, Math.ceil(cellW), ROW_H - 2);
      }
      ctx.fillStyle = '#5b6470';
      ctx.textAlign = 'right';
      ctx.fillText(row.lastFlip == null ? '–' : formatTime(row.lastFlip, zoneOffset), width - 2, y + ROW_H / 2);
      ctx.textAlign = 'left';
    }
  }, [state.version, state.rows, state.slots, order, viewport, selected, zoneOffset, state]);

  function onClick(event) {
    const rect = scrollRef.current.getBoundingClientRect();
    const i = Math.floor((event.clientY - rect.top + scrollRef.current.scrollTop) / ROW_H);
    if (i >= 0 && i < order.length) onSelect(state.rows[order[i]].symbol);
  }

  const spanStart = state.spanEnd == null ? null : state.spanEnd - (state.slots - 1) * state.slotMs;
  const mid = spanStart == null ? null : spanStart + (state.spanEnd - spanStart) / 2;

  return (
    <section className="panel heatmap">
      <div className="ptitle">
        <b>Trend heatmap</b>
        <span className="m">{order.length.toLocaleString('en')} symbols · sorted by latest sign flip · click a row to chart it</span>
      </div>
      <div className="filters">
        <Chips label="Exchange" values={EXCHANGES} value={filters.exchange} onChange={(exchange) => setFilters({ ...filters, exchange })} />
        <Chips label="Type" values={SEC_TYPES} value={filters.secType} onChange={(secType) => setFilters({ ...filters, secType })} />
      </div>
      <div className="hm-scroll" ref={scrollRef} onClick={onClick}>
        <div style={{ height: order.length * ROW_H }} />
        <canvas ref={canvasRef} className="hm-canvas" />
      </div>
      <div className="hm-axis" style={{ paddingLeft: LABEL_W, paddingRight: FLIP_W }}>
        <span>{formatTime(spanStart, zoneOffset)}</span>
        <span>{formatTime(mid, zoneOffset)}</span>
        <span>{formatTime(state.spanEnd, zoneOffset)}</span>
      </div>
      <div className="note legend-row">
        <i className="swatch" style={{ background: COLORS[UP] }} /> EMA38 &gt; EMA100
        <i className="swatch" style={{ background: COLORS[DOWN] }} /> EMA38 &lt; EMA100
        <i className="swatch" style={{ background: COLORS[EQUAL] }} /> equal
        <i className="swatch empty" /> no trades · last 24 h of 5-minute windows · right column: last flip
      </div>
    </section>
  );
}
