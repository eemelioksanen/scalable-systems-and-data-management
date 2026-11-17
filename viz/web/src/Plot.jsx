import { useEffect, useRef } from 'react';
import uPlot from 'uplot';

// A uPlot chart that fills its box. It is rebuilt when `options` changes and updated in place when
// `data` does, so memoise both. `redraw` forces a redraw for state the options read through refs.
export default function Plot({ options, data, redraw, className }) {
  const holder = useRef(null);
  const plot = useRef(null);

  useEffect(() => {
    const el = holder.current;
    const p = new uPlot({ ...options, width: el.clientWidth, height: el.clientHeight }, data, el);
    plot.current = p;
    const ro = new ResizeObserver(() => p.setSize({ width: el.clientWidth, height: el.clientHeight }));
    ro.observe(el);
    return () => {
      ro.disconnect();
      p.destroy();
      plot.current = null;
    };
    // Data changes go through setData below, not a rebuild.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [options]);

  // setData, not a bare redraw: before the first layout uPlot has no axes to redraw against.
  useEffect(() => {
    plot.current?.setData(data);
  }, [data, redraw]);

  return <div ref={holder} className={className} />;
}
