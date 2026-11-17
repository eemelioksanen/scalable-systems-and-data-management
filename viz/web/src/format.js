import { DOWN, EQUAL, UP } from './state.js';

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const pad = (n, w = 2) => String(n).padStart(w, '0');

export function offsetMs(zoneOffset) {
  const m = /^([+-])(\d{2}):(\d{2})$/.exec(zoneOffset ?? '');
  if (!m) return 0;
  const minutes = Number(m[2]) * 60 + Number(m[3]);
  return (m[1] === '-' ? -minutes : minutes) * 60000;
}

// Fields of an epoch-ms instant as seen at a fixed offset, independent of the browser's zone.
function fieldsAt(ms, zoneOffset) {
  const d = new Date(ms + offsetMs(zoneOffset));
  return {
    day: DAYS[d.getUTCDay()],
    date: `${pad(d.getUTCDate())}-${pad(d.getUTCMonth() + 1)}-${d.getUTCFullYear()}`,
    time: `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`,
    seconds: pad(d.getUTCSeconds()),
  };
}

export function formatDateTime(ms, zoneOffset) {
  if (ms == null) return '–';
  const f = fieldsAt(ms, zoneOffset);
  return `${f.day} ${f.date} ${f.time}:${f.seconds}`;
}

export function formatTime(ms, zoneOffset) {
  if (ms == null) return '–';
  return fieldsAt(ms, zoneOffset).time;
}

export function formatLocal(ms) {
  const d = new Date(ms);
  return `${DAYS[d.getDay()]} ${pad(d.getDate())}-${pad(d.getMonth() + 1)}-${d.getFullYear()} `
    + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

export function formatLag(ms) {
  if (ms == null || ms < 0) return null;
  const minutes = Math.round(ms / 60000);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  return `${hours} h ${minutes % 60} min`;
}

// A Date whose local fields equal the instant's fields at the offset, for uPlot's tzDate.
export function shiftedDate(seconds, zoneOffset) {
  const d = new Date(seconds * 1000 + offsetMs(zoneOffset));
  return new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(),
    d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds());
}

// An advisory's admission-to-publication latency, wall-clock (D-40).
export function formatLatency(ms) {
  if (ms == null) return '–';
  if (ms < 1000) return `${Math.max(0, Math.round(ms))} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  const s = Math.round(ms / 1000);
  return `${Math.floor(s / 60)} min ${s % 60} s`;
}

const IDLE_AFTER_MS = 10000;

// What the clock bar says about the results. Gaps are never shown as simulated minutes: those scale
// with the replay rate. A window is published once the next one has started (spec 3.1 (7)) and the
// watermark has passed it, so the newest published window always trails the feed.
export function clockStatus(clock, wall, slotMs, connection) {
  if (connection === 'reconnecting') return { key: 'off', text: 'reconnecting' };
  if (connection === 'connecting') return { key: 'off', text: 'connecting' };
  const feedMs = clock?.feedMs ?? null;
  const resultsMs = clock?.resultsMs ?? null;
  if (feedMs == null && resultsMs == null) return { key: 'off', text: 'waiting for Kafka' };
  const idle = clock?.feedIdleSince != null && wall - clock.feedIdleSince > IDLE_AFTER_MS;
  if (resultsMs == null) return { key: 'wait', text: idle ? 'feed idle, no results' : 'no results yet' };
  if (idle) return { key: 'idle', text: 'feed idle' };
  if (feedMs == null || feedMs - resultsMs <= slotMs) return { key: 'live', text: 'window open', nextEnd: resultsMs + slotMs };
  return { key: 'wait', text: 'waiting for the next window' };
}

export function formatPrice(v) {
  if (v == null || !Number.isFinite(v)) return '–';
  const a = Math.abs(v);
  return v.toFixed(a >= 1000 ? 1 : a >= 100 ? 2 : a >= 1 ? 3 : 4);
}

export function formatPct(v) {
  if (v == null || !Number.isFinite(v)) return '–';
  const a = Math.abs(v);
  return `${v >= 0 ? '+' : '−'}${a.toFixed(a >= 10 ? 1 : a >= 1 ? 2 : 3)}%`;
}

// How each trend of trendOf is shown. NONE has no entry.
export const TREND = {
  [UP]: { cls: 'up', arrow: '▲', word: 'BULL' },
  [DOWN]: { cls: 'dn', arrow: '▼', word: 'BEAR' },
  [EQUAL]: { cls: 'dim', arrow: '=', word: 'EVEN' },
};

export function formatMinute(ms, zoneOffset) {
  return ms == null ? '–' : formatDateTime(ms, zoneOffset).slice(0, -3);
}
