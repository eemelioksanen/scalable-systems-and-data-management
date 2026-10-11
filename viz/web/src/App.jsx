import { useEffect, useState } from 'react';
import ClockBar from './ClockBar.jsx';
import Heatmap from './Heatmap.jsx';
import SymbolChart from './SymbolChart.jsx';
import { useDashboardSocket } from './useDashboardSocket.js';
import { visibleRows } from './state.js';

const SUBSCRIBE = `echo 'all|{"trader":"all","symbols":["*"]}' | docker compose exec -T kafka \\
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic subscriptions \\
  --reader-property parse.key=true --reader-property key.separator='|'`;

function readHash() {
  return decodeURIComponent(window.location.hash.slice(1)) || null;
}

function NoResults() {
  return (
    <div className="empty-state">
      <h2>No results yet</h2>
      <p>
        Nothing has arrived on <code>ema-results</code>. The Flink job publishes only subscribed symbols, and it holds
        every result back until it has read the <code>subscriptions</code> topic. Write a subscription before submitting
        the job, for example one for every symbol:
      </p>
      <pre>{SUBSCRIBE}</pre>
      <p>Then submit the job and start a replay. The README has both commands.</p>
    </div>
  );
}

export default function App() {
  const [picked, setPicked] = useState(readHash);
  const [defaultSymbol, setDefaultSymbol] = useState(null);
  const selected = picked ?? defaultSymbol;
  const { state, status } = useDashboardSocket(selected);

  useEffect(() => {
    const onHash = () => setPicked(readHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    if (picked || defaultSymbol || state.rows.length === 0) return;
    const top = visibleRows(state.rows, {})[0];
    if (top !== undefined) setDefaultSymbol(state.rows[top].symbol);
  }, [picked, defaultSymbol, state.rows]);

  function select(symbol) {
    window.location.hash = encodeURIComponent(symbol);
    setPicked(symbol);
  }

  const zone = state.clock?.zoneOffset ?? '+02:00';
  const row = selected == null ? null : state.rows[state.index.get(selected)];
  const series = state.series?.symbol === selected ? state.series : null;

  return (
    <div className={status === 'live' ? 'app' : 'app dimmed'}>
      <ClockBar clock={state.clock} status={status} />
      {state.received && state.rows.length === 0 ? (
        <NoResults />
      ) : (
        <main className="grid">
          <Heatmap state={state} selected={selected} onSelect={select} zoneOffset={zone} />
          <SymbolChart series={series} row={row} slotMs={state.slotMs} slots={state.slots} zoneOffset={zone} />
        </main>
      )}
    </div>
  );
}
