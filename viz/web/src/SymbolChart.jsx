import { useEffect, useMemo, useRef } from 'react';
import uPlot from 'uplot';
import { toChartData } from './state.js';
import { formatTime, shiftedDate } from './format.js';

const INK = '#111418';
const EMA38 = '#2563eb';
const EMA100 = '#d97706';
const MUTED = '#5b6470';

// Draws the dashed marker and label on the symbol's first window: its BUY comes from EMA0 = 0 (D-7).
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
        ctx.strokeStyle = '#9aa3ad';
        ctx.setLineDash([3 * dpr, 3 * dpr]);
        ctx.beginPath();
        ctx.moveTo(x, u.bbox.top);
        ctx.lineTo(x, u.bbox.top + u.bbox.height);
        ctx.stroke();
        ctx.fillStyle = MUTED;
        ctx.font = `${11 * dpr}px -apple-system, sans-serif`;
        ctx.fillText('▲ first window: BUY from EMA₀ = 0', x + 4 * dpr, u.bbox.top + u.bbox.height - 6 * dpr);
        ctx.restore();
      },
    },
  };
}

export default function SymbolChart({ series, row, slotMs, slots, zoneOffset }) {
  const holderRef = useRef(null);
  const plotRef = useRef(null);
  const firstRef = useRef(null);
  const points = series?.points ?? [];
  const data = useMemo(() => toChartData(points, slotMs), [points, slotMs]);
  firstRef.current = series?.firstWindowEnd ?? null;

  useEffect(() => {
    const holder = holderRef.current;
    const sparse = (u) => u.data[0].length <= 2;
    const opts = {
      width: holder.clientWidth,
      height: holder.clientHeight,
      tzDate: (ts) => shiftedDate(ts, zoneOffset),
      legend: { show: false },
      cursor: { drag: { x: false, y: false } },
      scales: { x: { time: true } },
      axes: [
        { stroke: MUTED, grid: { stroke: '#eef0f3' }, ticks: { stroke: '#e3e6ea' } },
        { stroke: MUTED, grid: { stroke: '#eef0f3' }, ticks: { stroke: '#e3e6ea' }, size: 56 },
      ],
      series: [
        {},
        { label: 'Close', stroke: INK, width: 1, points: { show: sparse } },
        { label: 'EMA38', stroke: EMA38, width: 1.5, points: { show: sparse } },
        { label: 'EMA100', stroke: EMA100, width: 1.5, points: { show: sparse } },
      ],
      plugins: [firstWindowPlugin(() => firstRef.current)],
    };
    const plot = new uPlot(opts, data, holder);
    plotRef.current = plot;
    const ro = new ResizeObserver(() => plot.setSize({ width: holder.clientWidth, height: holder.clientHeight }));
    ro.observe(holder);
    return () => {
      ro.disconnect();
      plot.destroy();
      plotRef.current = null;
    };
    // The plot is rebuilt only when the zone changes; data updates go through setData below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [zoneOffset]);

  useEffect(() => {
    plotRef.current?.setData(data);
  }, [data]);

  const symbol = series?.symbol ?? row?.symbol;
  let empty = null;
  if (!symbol) empty = 'Select a symbol in the heatmap.';
  else if (series && points.length === 0) empty = 'No windows for this symbol in the last 24 h.';

  return (
    <section className="panel chart">
      <div className="ptitle">
        <b>{symbol ?? '–'}</b>
        {row && (
          <span className="m">
            {row.secType === 'I' ? 'index' : 'equity'} · {row.exchange} · {points.length} of {slots} windows with trades
          </span>
        )}
        <span className="sp" />
        <span className="legend">
          <span><i className="sw" style={{ background: INK }} />Close</span>
          <span><i className="sw" style={{ background: EMA38 }} />EMA38</span>
          <span><i className="sw" style={{ background: EMA100 }} />EMA100</span>
        </span>
      </div>
      <div className="chart-holder" ref={holderRef}>
        {empty && <div className="chart-empty">{empty}</div>}
      </div>
      <div className="note">
        Price currency is not published: <code>ticks</code> carries 7 of the 39 source columns (D-24) and the outputs carry none (D-16).
        The time axis is <code>windowEnd</code>, the exclusive end of each 5-minute window, at UTC{zoneOffset}.
        Windows without trades are skipped (D-6) and drawn as gaps. Latest point: {formatTime(points.at(-1)?.[0] ?? null, zoneOffset)}.
      </div>
    </section>
  );
}
