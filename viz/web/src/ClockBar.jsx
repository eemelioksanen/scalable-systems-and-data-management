import { useEffect, useState } from 'react';
import { clockStatus, formatDateTime, formatLocal, formatTime } from './format.js';

const TITLES = {
  live: 'The newest window is published and the next one is still open: it is evaluated once the window after it starts (spec 3.1 (7)).',
  wait: 'No newer window is published yet. The job waits for its watermark (the 1 s bound, or a quiet partition up to the 30 s idleness timeout), or no subscribed symbol traded since. Watermark lag is graphed in Grafana.',
  idle: 'No new ticks. Windows still open are evaluated when input resumes or the job is drained.',
  off: 'Not connected to results yet.',
};

export default function ClockBar({ clock, status, slotMs }) {
  const [wall, setWall] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setWall(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const zone = clock?.zoneOffset ?? '+02:00';
  const s = clockStatus(clock, wall, slotMs, status);
  const connected = status === 'live';

  return (
    <header className="bar">
      <span title="windowEnd of the newest published result: the exclusive end of its 5-minute window">
        <span className="k">Results as of</span>
        <span className="v">{formatDateTime(clock?.resultsMs ?? null, zone).slice(0, -3)}</span>
      </span>
      <span className={`pill state ${s.key}`} title={TITLES[s.key]}>
        {s.text}{s.nextEnd != null && <> to {formatTime(s.nextEnd, zone)}</>}
      </span>
      <span className="minor" title="Sampled from the tail of ticks once a second: the replay's source time, at the job's zone offset. It moves in steps at fast replay rates.">
        <span className="k">Feed</span>
        <span>{formatDateTime(clock?.feedMs ?? null, zone)}</span>
      </span>
      <span className="minor">
        <span className="k">Wall clock</span>
        <span>{formatLocal(wall)}</span>
      </span>
      <span className="sp" />
      <span className="pill" title="The Flink job's --zone-offset (D-3): feed and window times are shown at this offset">
        UTC{zone} (job --zone-offset)
      </span>
      <span className={connected ? 'pill live' : 'pill off'}>● {connected ? 'connected' : status}</span>
    </header>
  );
}
