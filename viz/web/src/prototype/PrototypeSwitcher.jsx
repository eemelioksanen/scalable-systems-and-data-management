// PROTOTYPE: variant switcher for the dashboard redesign. ?variant=A|B|C, arrows or ← → keys.
import { useEffect } from 'react';
import Terminal from './VariantTerminal.jsx';
import MarketMap from './VariantMarketMap.jsx';
import Focus from './VariantFocus.jsx';

export const VARIANTS = {
  current: { name: 'Current dashboard', component: null },
  A: { name: 'Terminal', component: Terminal },
  B: { name: 'Market map', component: MarketMap },
  C: { name: 'Focus', component: Focus },
};
const KEYS = Object.keys(VARIANTS);

export function readVariant() {
  const v = new URLSearchParams(window.location.search).get('variant');
  return KEYS.includes(v) ? v : 'A';
}

export function PrototypeSwitcher({ current, onChange }) {
  function go(step) {
    const next = KEYS[(KEYS.indexOf(current) + step + KEYS.length) % KEYS.length];
    const q = new URLSearchParams(window.location.search);
    q.set('variant', next);
    window.history.replaceState(null, '', `${window.location.pathname}?${q}${window.location.hash}`);
    onChange(next);
  }

  useEffect(() => {
    const onKey = (e) => {
      if (e.target.closest('input, textarea, select, [contenteditable]')) return;
      if (e.key === 'ArrowLeft') go(-1);
      if (e.key === 'ArrowRight') go(1);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  });

  if (import.meta.env.PROD) return null;
  return (
    <div className="proto-switch">
      <button type="button" onClick={() => go(-1)} aria-label="Previous variant">←</button>
      <span><b>{current}</b> {VARIANTS[current].name}</span>
      <button type="button" onClick={() => go(1)} aria-label="Next variant">→</button>
    </div>
  );
}
