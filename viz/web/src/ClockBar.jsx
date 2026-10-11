import { useEffect, useState } from 'react';
import { formatDateTime, formatLag, formatLocal, formatTime } from './format.js';

const IDLE_LABEL_AFTER_MS = 10000;

export default function ClockBar({ clock, status }) {
  const [wall, setWall] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setWall(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const zone = clock?.zoneOffset ?? '+02:00';
  const feedMs = clock?.feedMs ?? null;
  const resultsMs = clock?.resultsMs ?? null;
  const lag = feedMs != null && resultsMs != null ? formatLag(feedMs - resultsMs) : null;
  const idle = clock?.feedIdleSince != null && wall - clock.feedIdleSince > IDLE_LABEL_AFTER_MS;
  const waitingForKafka = status === 'live' && feedMs == null && resultsMs == null;

  let pill = { text: 'live', cls: 'pill live' };
  if (status === 'reconnecting') pill = { text: 'reconnecting', cls: 'pill off' };
  else if (status === 'connecting') pill = { text: 'connecting', cls: 'pill off' };
  else if (waitingForKafka) pill = { text: 'waiting for Kafka', cls: 'pill off' };

  return (
    <header className="bar">
      <span title="Latest replayed tick, from the Date and Time of the newest ticks records">
        <span className="k">Feed time</span>
        <span className="v">{formatDateTime(feedMs, zone)}</span>
        {idle && <span className="gap"> feed idle</span>}
      </span>
      <span title="windowEnd of the newest ema-results record: the exclusive end of its 5-minute window">
        <span className="k">Latest window</span>
        <span className="v">{formatTime(resultsMs, zone)}</span>
        {lag && <span className="gap"> (results {lag} behind feed)</span>}
      </span>
      <span>
        <span className="k">Wall clock</span>
        <span className="v">{formatLocal(wall)}</span>
      </span>
      <span className="sp" />
      <span className="pill" title="The Flink job's --zone-offset (D-3): feed and window times are shown at this offset">
        UTC{zone} (job --zone-offset)
      </span>
      <span className={pill.cls}>● {pill.text}</span>
    </header>
  );
}
