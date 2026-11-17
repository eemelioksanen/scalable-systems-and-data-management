// PROTOTYPE: charts and helpers shared by the redesign variants. Every chart reads published
// results only; nothing here recomputes an EMA or a crossover.
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import { DOWN, UP, cellAt, toChartData } from '../state.js';
import { formatTime, shiftedDate } from '../format.js';

export function useFonts(href) {
  useEffect(() => {
    if (document.querySelector(`link[href="${href}"]`)) return;
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = href;
    document.head.appendChild(link);
  }, [href]);
}

export function useWidth() {
  const ref = useRef(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useLayoutEffect(() => {
    const el = ref.current;
    const ro = new ResizeObserver(() => setSize({ w: el.clientWidth, h: el.clientHeight }));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, size];
}

export const pct = (v, digits = 2) => (v == null || !Number.isFinite(v) ? '–' : `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`);
export const price = (v) => (v == null ? '–' : v >= 1000 ? v.toFixed(1) : v >= 100 ? v.toFixed(2) : v.toFixed(3));
export const change = (q) => (q ? ((q.close - q.open) / q.open) * 100 : null);
export const spreadPct = (q) => (q ? ((q.ema38 - q.ema100) / q.ema100) * 100 : null);

// Share of symbols whose EMA38 is above EMA100, per window of the span. Counts only symbols with a
// window in that slot, so a quiet symbol does not drag the line down.
export function breadth(state, rowIdx) {
  const xs = [];
  const share = [];
  const counted = [];
  for (let c = 0; c < state.slots; c++) {
    let up = 0;
    let n = 0;
    for (const r of rowIdx) {
      const v = cellAt(state, r, c);
      if (v === UP) up++;
      if (v === UP || v === DOWN) n++;
    }
    xs.push(state.spanEnd - (state.slots - 1 - c) * state.slotMs);
    share.push(n ? up / n : null);
    counted.push(n);
  }
  return { xs, share, counted };
}

export function Sparkline({ values, overlay, width = 96, height = 28, color, overlayColor, fill }) {
  if (!values || values.length < 2) return <svg width={width} height={height} />;
  const all = overlay ? values.concat(overlay) : values;
  const lo = Math.min(...all);
  const hi = Math.max(...all);
  const span = hi - lo || 1;
  const pts = (vs) => vs.map((v, i) => `${(i / (vs.length - 1)) * width},${height - 2 - ((v - lo) / span) * (height - 4)}`).join(' ');
  const line = pts(values);
  return (
    <svg width={width} height={height} className="spark" aria-hidden="true">
      {fill && <polygon points={`0,${height} ${line} ${width},${height}`} fill={fill} />}
      {overlay && <polyline points={pts(overlay)} fill="none" stroke={overlayColor} strokeWidth="1" strokeDasharray="2 2" />}
      <polyline points={line} fill="none" stroke={color} strokeWidth="1.4" strokeLinejoin="round" />
    </svg>
  );
}

// The last `columns` heatmap cells of one row: the real EMA38 vs EMA100 sign history.
export function SignStrip({ state, row, columns = 72, width = 120, height = 10, up, down, empty }) {
  const w = width / columns;
  const cells = [];
  for (let i = 0; i < columns; i++) {
    const v = cellAt(state, row, state.slots - columns + i);
    cells.push(<rect key={i} x={i * w} y={0} width={Math.max(1, w - 0.6)} height={height} fill={v === UP ? up : v === DOWN ? down : empty} />);
  }
  return <svg width={width} height={height} className="strip" aria-hidden="true">{cells}</svg>;
}

function markerPlugin(getMarkers, theme) {
  return {
    hooks: {
      draw: (u) => {
        const { ctx } = u;
        const dpr = window.devicePixelRatio || 1;
        const r = 6 * dpr;
        ctx.save();
        for (const m of getMarkers()) {
          const x = u.valToPos(m.windowEnd / 1000, 'x', true);
          const y = u.valToPos(m.ema38, 'y', true);
          if (x < u.bbox.left || x > u.bbox.left + u.bbox.width) continue;
          const c = m.type === 'BUY' ? theme.buy : theme.sell;
          ctx.strokeStyle = c;
          ctx.globalAlpha = 0.35;
          ctx.setLineDash([2 * dpr, 3 * dpr]);
          ctx.beginPath();
          ctx.moveTo(x, u.bbox.top);
          ctx.lineTo(x, u.bbox.top + u.bbox.height);
          ctx.stroke();
          ctx.setLineDash([]);
          ctx.globalAlpha = 1;
          const dir = m.type === 'BUY' ? 1 : -1;
          const by = y + dir * (r * 2.2);
          ctx.beginPath();
          ctx.moveTo(x, by - dir * r);
          ctx.lineTo(x + r, by + dir * r * 0.7);
          ctx.lineTo(x - r, by + dir * r * 0.7);
          ctx.closePath();
          if (m.firstWindow) {
            ctx.lineWidth = 1.5 * dpr;
            ctx.stroke();
          } else {
            ctx.fillStyle = c;
            ctx.fill();
          }
          ctx.beginPath();
          ctx.arc(x, y, 3 * dpr, 0, Math.PI * 2);
          ctx.fillStyle = theme.bg;
          ctx.fill();
          ctx.lineWidth = 1.5 * dpr;
          ctx.strokeStyle = c;
          ctx.stroke();
        }
        ctx.restore();
      },
    },
  };
}

function vGradient(u, top, bottom) {
  const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
  g.addColorStop(0, top);
  g.addColorStop(1, bottom);
  return g;
}

// Gradient split at zero: up colour above the axis, down colour below.
function zeroSplit(u, up, down) {
  const zero = u.valToPos(0, 'y', true);
  const f = Math.min(1, Math.max(0, (zero - u.bbox.top) / u.bbox.height));
  const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
  g.addColorStop(0, up);
  g.addColorStop(f, up);
  g.addColorStop(f, down);
  g.addColorStop(1, down);
  return g;
}

function UPlot({ build, data, className }) {
  const holder = useRef(null);
  const plot = useRef(null);
  useEffect(() => {
    const el = holder.current;
    const p = new uPlot({ ...build(), width: el.clientWidth, height: el.clientHeight }, data, el);
    plot.current = p;
    const ro = new ResizeObserver(() => p.setSize({ width: el.clientWidth, height: el.clientHeight }));
    ro.observe(el);
    return () => {
      ro.disconnect();
      p.destroy();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [build]);
  useEffect(() => {
    plot.current?.setData(data);
  }, [data]);
  return <div ref={holder} className={className} />;
}

function axes(theme, zone, ySize = 58, yFmt) {
  const ax = { stroke: theme.axis, grid: { stroke: theme.grid, width: 1 }, ticks: { show: false }, font: theme.axisFont };
  return [
    { ...ax, values: (u, ts) => ts.map((t) => formatTime(t * 1000, zone)) },
    { ...ax, side: 1, size: ySize, values: yFmt ? (u, vs) => vs.map(yFmt) : undefined },
  ];
}

export function chartData(points, slotMs, hours) {
  const pts = hours ? points.filter((p) => p[0] > (points.at(-1)?.[0] ?? 0) - hours * 3600000) : points;
  const [xs, close, e38, e100] = toChartData(pts, slotMs);
  const spread = e38.map((v, i) => (v == null || e100[i] == null ? null : ((v - e100[i]) / e100[i]) * 100));
  return { xs, close, e38, e100, spread };
}

// Close, EMA38 and EMA100 with the published advisories as markers. onCursor gets the hovered index.
export function PriceChart({ d, markers, theme, zone, onCursor, area = true, className = 'pc', syncKey = 'proto' }) {
  const markersRef = useRef(markers);
  markersRef.current = markers;
  const cursorRef = useRef(onCursor);
  cursorRef.current = onCursor;
  const build = useMemo(() => () => ({
    legend: { show: false },
    tzDate: (ts) => shiftedDate(ts, zone),
    cursor: { sync: { key: syncKey }, drag: { x: false, y: false }, points: { size: 7, fill: theme.bg } },
    scales: { x: { time: true } },
    axes: axes(theme, zone, 64, price),
    series: [
      {},
      { stroke: theme.close, width: 1.6, points: { show: false }, fill: area ? (u) => vGradient(u, theme.areaTop, theme.areaBottom) : undefined },
      { stroke: theme.ema38, width: 1.3, points: { show: false } },
      { stroke: theme.ema100, width: 1.3, dash: theme.dash100, points: { show: false } },
    ],
    hooks: { setCursor: [(u) => cursorRef.current?.(u.cursor.idx)] },
    plugins: [markerPlugin(() => markersRef.current, theme)],
  }), [theme, zone, area, syncKey]);
  const data = useMemo(() => [d.xs, d.close, d.e38, d.e100], [d]);
  return <UPlot build={build} data={data} className={className} />;
}

// EMA38 − EMA100 as a share of EMA100. A crossover is where it changes sign, so the advisories sit
// on the zero line by construction.
export function SpreadChart({ d, theme, zone, className = 'sc', syncKey = 'proto' }) {
  const build = useMemo(() => () => ({
    legend: { show: false },
    tzDate: (ts) => shiftedDate(ts, zone),
    cursor: { sync: { key: syncKey }, drag: { x: false, y: false }, points: { show: false } },
    scales: { x: { time: true } },
    axes: axes(theme, zone, 64, (v) => `${v.toFixed(2)}%`),
    series: [
      {},
      {
        stroke: (u) => zeroSplit(u, theme.buy, theme.sell),
        fill: (u) => zeroSplit(u, theme.buyFill, theme.sellFill),
        fillTo: 0,
        width: 1.2,
        points: { show: false },
      },
    ],
  }), [theme, zone, syncKey]);
  const data = useMemo(() => [d.xs, d.spread], [d]);
  return <UPlot build={build} data={data} className={className} />;
}

export function BreadthChart({ b, theme, zone, className = 'bc' }) {
  const build = useMemo(() => () => ({
    legend: { show: false },
    tzDate: (ts) => shiftedDate(ts, zone),
    cursor: { drag: { x: false, y: false }, points: { show: false } },
    scales: { x: { time: true }, y: { range: [0, 100] } },
    axes: axes(theme, zone, 40, (v) => `${v}%`),
    series: [
      {},
      {
        stroke: (u) => {
          const mid = u.valToPos(50, 'y', true);
          const f = (mid - u.bbox.top) / u.bbox.height;
          const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
          g.addColorStop(0, theme.buy); g.addColorStop(f, theme.buy); g.addColorStop(f, theme.sell); g.addColorStop(1, theme.sell);
          return g;
        },
        fill: (u) => vGradient(u, theme.buyFill, 'transparent'),
        width: 1.5,
        paths: uPlot.paths.stepped({ align: 1 }),
      },
    ],
  }), [theme, zone]);
  const data = useMemo(() => [b.xs.map((x) => x / 1000), b.share.map((v) => (v == null ? null : v * 100))], [b]);
  return <UPlot build={build} data={data} className={className} />;
}

// Every advisory in the span on one time axis, one lane per symbol that has any.
export function CrossoverTimeline({ advisories, state, theme, zone, selected, onSelect, laneH = 18, labelW = 92 }) {
  const [ref, { w }] = useWidth();
  const lanes = [...new Set(advisories.map((a) => a.symbol))].sort();
  const start = state.spanEnd - state.slots * state.slotMs;
  const x = (t) => labelW + ((t - start) / (state.slots * state.slotMs)) * Math.max(0, w - labelW - 8);
  const ticks = [];
  for (let k = 0; k <= 6; k++) ticks.push(start + (k * state.slots * state.slotMs) / 6);
  const h = lanes.length * laneH + 22;
  return (
    <div ref={ref} className="timeline">
      {w > 0 && (
        <svg width={w} height={h}>
          {ticks.map((t) => (
            <g key={t}>
              <line x1={x(t)} x2={x(t)} y1={0} y2={h - 18} stroke={theme.grid} />
              <text x={x(t)} y={h - 4} fill={theme.axis} fontSize="10" textAnchor="middle">{formatTime(t, zone)}</text>
            </g>
          ))}
          {lanes.map((s, i) => (
            <g key={s} onClick={() => onSelect(s)} style={{ cursor: 'pointer' }}>
              <rect x={0} y={i * laneH} width={w} height={laneH} fill={s === selected ? theme.selRow : 'transparent'} />
              <line x1={labelW} x2={w - 8} y1={i * laneH + laneH / 2} y2={i * laneH + laneH / 2} stroke={theme.grid} />
              <text x={4} y={i * laneH + laneH / 2 + 4} fill={s === selected ? theme.ink : theme.axis} fontSize="11" fontWeight={s === selected ? 600 : 400}>{s}</text>
              {advisories.filter((a) => a.symbol === s).map((a) => {
                const cx = x(a.windowEnd);
                const cy = i * laneH + laneH / 2;
                const c = a.type === 'BUY' ? theme.buy : theme.sell;
                const p = a.type === 'BUY' ? `${cx},${cy - 6} ${cx + 6},${cy + 4} ${cx - 6},${cy + 4}` : `${cx},${cy + 6} ${cx + 6},${cy - 4} ${cx - 6},${cy - 4}`;
                return (
                  <polygon key={a.windowEnd} points={p} fill={a.firstWindow ? 'none' : c} stroke={c} strokeWidth="1.3">
                    <title>{`${a.type} ${a.symbol} ${formatTime(a.windowEnd, zone)}`}</title>
                  </polygon>
                );
              })}
            </g>
          ))}
        </svg>
      )}
    </div>
  );
}

// Advisory latency (emitTs − maxIngestTs) per advisory, on a log axis from 10 ms to 1 min.
export function LatencyStrip({ advisories, theme, height = 90 }) {
  const [ref, { w }] = useWidth();
  const pts = advisories.filter((a) => a.latencyMs != null);
  const sorted = pts.map((a) => a.latencyMs).sort((a, b) => a - b);
  const q = (p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : null);
  const lo = Math.log10(10);
  const hi = Math.log10(60000);
  const x = (ms) => 8 + ((Math.log10(Math.max(10, ms)) - lo) / (hi - lo)) * Math.max(0, w - 16);
  const ticks = [10, 100, 1000, 10000, 60000];
  const label = (ms) => (ms < 1000 ? `${ms} ms` : `${ms / 1000} s`);
  return (
    <div ref={ref} className="latency">
      {w > 0 && (
        <svg width={w} height={height}>
          {ticks.map((t) => (
            <g key={t}>
              <line x1={x(t)} x2={x(t)} y1={8} y2={height - 20} stroke={theme.grid} />
              <text x={x(t)} y={height - 6} fill={theme.axis} fontSize="10" textAnchor={t === 10 ? 'start' : t === 60000 ? 'end' : 'middle'}>{label(t)}</text>
            </g>
          ))}
          {q(0.5) != null && <line x1={x(q(0.5))} x2={x(q(0.5))} y1={4} y2={height - 20} stroke={theme.ink} strokeDasharray="3 3" />}
          {pts.map((a, i) => (
            <circle key={`${a.symbol}${a.windowEnd}`} cx={x(a.latencyMs)} cy={16 + ((i * 37) % (height - 44))} r="4"
              fill={a.type === 'BUY' ? theme.buy : theme.sell} fillOpacity="0.85">
              <title>{`${a.symbol} ${a.type}: ${a.latencyMs} ms`}</title>
            </circle>
          ))}
        </svg>
      )}
    </div>
  );
}

export function latencyStats(advisories) {
  const s = advisories.map((a) => a.latencyMs).filter((v) => v != null).sort((a, b) => a - b);
  if (!s.length) return null;
  return { p50: s[Math.floor(s.length * 0.5)], p95: s[Math.min(s.length - 1, Math.floor(s.length * 0.95))], max: s.at(-1), n: s.length };
}

// BUY and SELL counts per hour of the span, BUY up and SELL down.
export function HourBars({ advisories, state, theme, zone, height = 110 }) {
  const [ref, { w }] = useWidth();
  const hours = Math.round((state.slots * state.slotMs) / 3600000);
  const start = state.spanEnd - hours * 3600000;
  const buy = new Array(hours).fill(0);
  const sell = new Array(hours).fill(0);
  for (const a of advisories) {
    const i = Math.floor((a.windowEnd - start - 1) / 3600000);
    if (i < 0 || i >= hours) continue;
    if (a.type === 'BUY') buy[i]++;
    else sell[i]++;
  }
  const max = Math.max(1, ...buy, ...sell);
  const mid = (height - 16) / 2;
  const bw = w / hours;
  return (
    <div ref={ref} className="hours">
      {w > 0 && (
        <svg width={w} height={height}>
          <line x1={0} x2={w} y1={mid} y2={mid} stroke={theme.grid} />
          {buy.map((n, i) => n > 0 && <rect key={`b${i}`} x={i * bw + 1} y={mid - (n / max) * (mid - 4)} width={bw - 2} height={(n / max) * (mid - 4)} rx="2" fill={theme.buy} />)}
          {sell.map((n, i) => n > 0 && <rect key={`s${i}`} x={i * bw + 1} y={mid} width={bw - 2} height={(n / max) * (mid - 4)} rx="2" fill={theme.sell} />)}
          {[0, 6, 12, 18].map((k) => (
            <text key={k} x={k * bw} y={height - 2} fill={theme.axis} fontSize="10">{formatTime(start + k * 3600000, zone)}</text>
          ))}
        </svg>
      )}
    </div>
  );
}
