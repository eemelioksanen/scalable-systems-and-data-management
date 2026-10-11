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
