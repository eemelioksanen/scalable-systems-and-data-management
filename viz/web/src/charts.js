import uPlot from 'uplot';
import { formatTime, shiftedDate } from './format.js';

// The palette of styles.css, for canvases. BUY/UP and SELL/DOWN share a colour everywhere.
export const C = {
  bg: '#0a0d14',
  ink: '#e9edf3',
  muted: '#8891a1',
  axis: '#5d6678',
  grid: 'rgba(136, 145, 161, 0.10)',
  accent: '#5ab9fc',
  ema38: '#5ab9fc',
  ema100: '#f5b84a',
  equal: '#5d6678',
  up: '#2ec4a0',
  down: '#f0616d',
  upFill: 'rgba(46, 196, 160, 0.22)',
  downFill: 'rgba(240, 97, 109, 0.22)',
  areaTop: 'rgba(90, 185, 252, 0.16)',
  areaBottom: 'rgba(90, 185, 252, 0)',
};
export const MONO = '"Geist Mono Variable", ui-monospace, SFMono-Regular, Menlo, monospace';

const NO_POINTS = { show: false };

function axes(zone, yFormat) {
  const ax = { stroke: C.axis, grid: { stroke: C.grid, width: 1 }, ticks: { show: false }, font: `10px ${MONO}` };
  return [
    { ...ax, values: (u, ts) => ts.map((t) => formatTime(t * 1000, zone)) },
    { ...ax, side: 1, size: 64, values: (u, vs) => vs.map(yFormat) },
  ];
}

// getSpan gives the x range in seconds, so every chart of the panel shares one time axis.
function base(zone, yFormat, getSpan, syncKey) {
  return {
    legend: { show: false },
    tzDate: (ts) => shiftedDate(ts, zone),
    cursor: { sync: syncKey ? { key: syncKey } : undefined, drag: { x: false, y: false }, points: { size: 7, fill: C.bg } },
    scales: { x: { time: true, range: (u, min, max) => getSpan() ?? [min, max] } },
    axes: axes(zone, yFormat),
  };
}

function verticalGradient(u, top, bottom) {
  const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
  g.addColorStop(0, top);
  g.addColorStop(1, bottom);
  return g;
}

// A hard colour change at value `at` of the y scale: `above` over it, `below` under it.
function splitAt(u, at, above, below) {
  const y = u.valToPos(at, 'y', true);
  const f = Math.min(1, Math.max(0, (y - u.bbox.top) / u.bbox.height));
  const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
  g.addColorStop(0, above);
  g.addColorStop(f, above);
  g.addColorStop(f, below);
  g.addColorStop(1, below);
  return g;
}

// A published advisory: a ring on EMA38 at its windowEnd, a triangle beside it (▲ BUY below the
// line, ▼ SELL above it) and a faint vertical guide. Its values come from the advisory record, not
// the line. A first-window BUY is hollow: published, but from EMA0 = 0, not a breakout (D-7).
function advisoryPlugin(getMarkers) {
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
          const color = m.type === 'BUY' ? C.up : C.down;
          ctx.strokeStyle = color;
          ctx.globalAlpha = 0.35;
          ctx.lineWidth = dpr;
          ctx.setLineDash([2 * dpr, 3 * dpr]);
          ctx.beginPath();
          ctx.moveTo(x, u.bbox.top);
          ctx.lineTo(x, u.bbox.top + u.bbox.height);
          ctx.stroke();
          ctx.setLineDash([]);
          ctx.globalAlpha = 1;
          const dir = m.type === 'BUY' ? 1 : -1;
          const ty = y + dir * r * 2.2;
          ctx.beginPath();
          ctx.moveTo(x, ty - dir * r);
          ctx.lineTo(x + r, ty + dir * r * 0.7);
          ctx.lineTo(x - r, ty + dir * r * 0.7);
          ctx.closePath();
          ctx.lineWidth = 1.5 * dpr;
          if (m.firstWindow) {
            ctx.stroke();
          } else {
            ctx.fillStyle = color;
            ctx.fill();
          }
          ctx.beginPath();
          ctx.arc(x, y, 3 * dpr, 0, Math.PI * 2);
          ctx.fillStyle = C.bg;
          ctx.fill();
          ctx.stroke();
        }
        ctx.restore();
      },
    },
  };
}

// The dashed line and note on a symbol's first window, where its BUY comes from EMA0 = 0 (D-7).
function firstWindowPlugin(getFirst) {
  return {
    hooks: {
      draw: (u) => {
        const first = getFirst();
        if (first == null) return;
        const x = u.valToPos(first / 1000, 'x', true);
        if (x < u.bbox.left || x > u.bbox.left + u.bbox.width) return;
        const { ctx } = u;
        const dpr = window.devicePixelRatio || 1;
        ctx.save();
        ctx.strokeStyle = C.muted;
        ctx.setLineDash([3 * dpr, 3 * dpr]);
        ctx.beginPath();
        ctx.moveTo(x, u.bbox.top);
        ctx.lineTo(x, u.bbox.top + u.bbox.height);
        ctx.stroke();
        ctx.fillStyle = C.muted;
        ctx.font = `${10 * dpr}px ${MONO}`;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'alphabetic';
        ctx.fillText('first window: BUY from EMA₀ = 0', x + 4 * dpr, u.bbox.top + u.bbox.height - 6 * dpr);
        ctx.restore();
      },
    },
  };
}

export function priceOptions({ zone, format, getSpan, getMarkers, getFirst, onCursor }) {
  return {
    ...base(zone, format, getSpan, 'symbol'),
    series: [
      {},
      { stroke: C.ink, width: 1.6, fill: (u) => verticalGradient(u, C.areaTop, C.areaBottom) },
      { stroke: C.ema38, width: 1.3 },
      { stroke: C.ema100, width: 1.3, dash: [4, 3] },
    ],
    hooks: { setCursor: [(u) => onCursor(u.cursor.idx)] },
    plugins: [firstWindowPlugin(getFirst), advisoryPlugin(getMarkers)],
  };
}

export function spreadOptions({ zone, format, getSpan }) {
  return {
    ...base(zone, format, getSpan, 'symbol'),
    cursor: { sync: { key: 'symbol' }, drag: { x: false, y: false }, points: NO_POINTS },
    series: [
      {},
      {
        stroke: (u) => splitAt(u, 0, C.up, C.down),
        fill: (u) => splitAt(u, 0, C.upFill, C.downFill),
        fillTo: 0,
        width: 1.2,
      },
    ],
  };
}

export function breadthOptions({ zone, getSpan }) {
  const opts = base(zone, (v) => `${v}%`, getSpan, 'symbol');
  return {
    ...opts,
    cursor: { sync: { key: 'symbol' }, drag: { x: false, y: false }, points: NO_POINTS },
    scales: { x: opts.scales.x, y: { range: [0, 100] } },
    series: [
      {},
      {
        stroke: (u) => splitAt(u, 50, C.up, C.down),
        fill: (u) => verticalGradient(u, C.upFill, 'rgba(0, 0, 0, 0)'),
        width: 1.5,
        points: NO_POINTS,
        paths: uPlot.paths.stepped({ align: 1 }),
      },
    ],
  };
}
