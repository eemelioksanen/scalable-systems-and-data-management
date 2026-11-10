import { useEffect, useMemo, useState } from 'react';
import AdvisoryFeed from './AdvisoryFeed.jsx';
import ClockBar from './ClockBar.jsx';
import Filters from './Filters.jsx';
import Heatmap from './Heatmap.jsx';
import SymbolChart from './SymbolChart.jsx';
import { useDashboardSocket } from './useDashboardSocket.js';
import { feedItems, indexAdvisories, visibleRows } from './state.js';

const SUBSCRIBE = `echo 'all|{"trader":"all","symbols":["*"]}' | docker compose exec -T kafka \\
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic subscriptions \\
  --reader-property parse.key=true --reader-property key.separator='|'`;

function readHash() {
  return decodeURIComponent(window.location.hash.slice(1)) || null;
}

const SORTS = ['cross', 'flip', 'symbol'];

// Filters and sort in the query string, so a view can be linked or captured: ?exchange=NL&trader=t1&sort=flip
function readQuery() {
  const q = new URLSearchParams(window.location.search);
  return {
    filters: { exchange: q.get('exchange'), secType: q.get('secType'), trader: q.get('trader') },
    sort: SORTS.includes(q.get('sort')) ? q.get('sort') : 'cross',
  };
}

function writeQuery(filters, sort) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) if (v) q.set(k, v);
  if (sort !== 'cross') q.set('sort', sort);
  const search = q.toString();
  window.history.replaceState(null, '', `${window.location.pathname}${search ? `?${search}` : ''}${window.location.hash}`);
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
  const [{ filters, sort }, setView] = useState(readQuery);
  const selected = picked ?? defaultSymbol;
  const { state, status } = useDashboardSocket(selected);
  const adv = useMemo(() => indexAdvisories(state.advisories), [state.advisories]);
  const order = useMemo(
    () => visibleRows(state.rows, filters, { sort, lastCross: adv.lastCross, traders: state.traders }),
    [state.rows, filters, sort, adv, state.traders],
  );
  const feed = useMemo(() => feedItems(adv.all, filters, state.traders), [adv, filters, state.traders]);

  useEffect(() => writeQuery(filters, sort), [filters, sort]);

  useEffect(() => {
    const onHash = () => setPicked(readHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    if (picked || defaultSymbol || order.length === 0) return;
    setDefaultSymbol(state.rows[order[0]].symbol);
  }, [picked, defaultSymbol, order, state.rows]);

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
        <>
          <Filters filters={filters} traders={state.traders} onChange={(f) => setView({ filters: f, sort })} />
          <main className="grid">
            <Heatmap state={state} order={order} sort={sort} onSort={(s) => setView({ filters, sort: s })} adv={adv}
              selected={selected} onSelect={select} zoneOffset={zone} />
            <div className="right">
              <SymbolChart series={series} row={row} markers={selected ? adv.bySymbol.get(selected) ?? [] : []}
                slotMs={state.slotMs} slots={state.slots} zoneOffset={zone} />
              <AdvisoryFeed items={feed} total={adv.all.length} selected={selected} onSelect={select} zoneOffset={zone} />
            </div>
          </main>
        </>
      )}
    </div>
  );
}
