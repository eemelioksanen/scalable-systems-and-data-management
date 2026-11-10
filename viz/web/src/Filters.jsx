const EXCHANGES = ['FR', 'NL', 'ETR'];
const SEC_TYPES = ['E', 'I'];

export function Chips({ label, values, value, onChange, labels = {} }) {
  return (
    <span className="chips">
      {label}
      <button type="button" className={value == null ? 'chip on' : 'chip'} onClick={() => onChange(null)}>All</button>
      {values.map((v) => (
        <button key={v} type="button" className={value === v ? 'chip on' : 'chip'} onClick={() => onChange(v)}>
          {labels[v] ?? v}
        </button>
      ))}
    </span>
  );
}

function describeList(list) {
  if (!list) return '';
  if (list.includes('*')) return 'all symbols';
  return `${list.length} symbol${list.length === 1 ? '' : 's'}`;
}

// Exchange, SecType and subscription filters, shared by the heatmap and the advisory feed.
export default function Filters({ filters, onChange, traders }) {
  const names = Object.keys(traders).sort();
  const missing = filters.trader && !traders[filters.trader];
  return (
    <div className="filters bar-filters">
      <Chips label="Exchange" values={EXCHANGES} value={filters.exchange} onChange={(exchange) => onChange({ ...filters, exchange })} />
      <Chips label="Type" values={SEC_TYPES} value={filters.secType} labels={{ E: 'equity', I: 'index' }}
        onChange={(secType) => onChange({ ...filters, secType })} />
      <label className="chips" title="A trader's current list in the subscriptions topic (D-17)">
        Subscription
        <select value={filters.trader ?? ''} onChange={(e) => onChange({ ...filters, trader: e.target.value || null })}>
          <option value="">All published symbols</option>
          {missing && <option value={filters.trader}>{filters.trader} (removed)</option>}
          {names.map((t) => <option key={t} value={t}>{t} · {describeList(traders[t])}</option>)}
        </select>
      </label>
      <span className="m">{names.length} trader{names.length === 1 ? '' : 's'} in <code>subscriptions</code></span>
    </div>
  );
}
