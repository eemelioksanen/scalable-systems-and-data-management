import { useEffect, useMemo, useState } from 'react';
import Signals from './Signals.jsx';
import SymbolPanel from './SymbolPanel.jsx';
import TopBar, { Ticker } from './TopBar.jsx';
import Watchlist from './Watchlist.jsx';
import { useDashboardSocket } from './useDashboardSocket.js';
import { breadth, feedItems, indexAdvisories, quantile, visibleRows } from './state.js';

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
    filters: { exchange: q.get('exchange'), secType: q.get('secType'), trader: q.get('trader'), firstWindows: q.get('first') === '1' },
    sort: SORTS.includes(q.get('sort')) ? q.get('sort') : 'cross',
  };
}

function writeQuery(filters, sort) {
  const q = new URLSearchParams();
  for (const k of ['exchange', 'secType', 'trader']) if (filters[k]) q.set(k, filters[k]);
  if (filters.firstWindows) q.set('first', '1');
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
  const { state, status, arrivals } = useDashboardSocket(selected);
  const adv = useMemo(() => indexAdvisories(state.advisories, { firstWindows: filters.firstWindows }),
    [state.advisories, filters.firstWindows]);
  const order = useMemo(
    () => visibleRows(state.rows, filters, { sort, lastCross: adv.lastCross, traders: state.traders }),
    [state.rows, filters, sort, adv, state.traders],
  );
  const feed = useMemo(() => feedItems(adv.all, filters, state.traders), [adv, filters, state.traders]);
  const latencies = useMemo(() => feed.map((a) => a.latencyMs).filter((v) => v != null).sort((a, b) => a - b), [feed]);
  // state.version moves on every change to the signs.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const share = useMemo(() => breadth(state, order), [state.version, order]);
  const buys = feed.filter((a) => a.type === 'BUY').length;

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

  const setFilters = (f) => setView({ filters: f, sort });

  return (
    <div className={status === 'live' ? 'app' : 'app dimmed'}>
      <TopBar clock={state.clock} status={status} slotMs={state.slotMs} breadthNow={share.findLast((v) => v != null) ?? null}
        buys={buys} sells={feed.length - buys} p50={quantile(latencies, 0.5)} />
      {state.received && state.rows.length === 0 ? (
        <NoResults />
      ) : (
        <>
          <Ticker rows={state.rows} order={order} selected={selected} onSelect={select} />
          <Watchlist state={state} order={order} sort={sort} onSort={(s) => setView({ filters, sort: s })}
            filters={filters} onFilters={setFilters} adv={adv} showFirst={filters.firstWindows}
            selected={selected} onSelect={select} zoneOffset={zone} />
          <SymbolPanel state={state} series={series} row={row} share={share}
            markers={selected ? adv.bySymbol.get(selected) ?? [] : []} zoneOffset={zone} />
          <Signals items={feed} arrivals={arrivals} latencies={latencies} total={adv.all.length}
            hiddenFirst={filters.firstWindows ? 0 : adv.all.filter((a) => a.firstWindow).length}
            showFirst={filters.firstWindows} onShowFirst={(firstWindows) => setFilters({ ...filters, firstWindows })}
            selected={selected} onSelect={select} zoneOffset={zone} />
        </>
      )}
      <footer className="foot">
        <span>ema-results · advisories</span>
        <span>{state.rows.length.toLocaleString('en')} symbols published</span>
      </footer>
    </div>
  );
}
