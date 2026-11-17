import { describe, expect, it } from 'vitest';
import {
  clockStatus, formatDateTime, formatLag, formatLatency, formatPct, formatPrice, formatTime, offsetMs, shiftedDate,
} from './format.js';

const T0 = 1636355100000; // 09:05 on 08-11-2021 at +02:00 (docs/interfaces.md)

describe('format', () => {
  it('formats at the job zone offset regardless of the browser zone', () => {
    expect(formatDateTime(T0, '+02:00')).toBe('Mon 08-11-2021 09:05:00');
    expect(formatTime(T0, '+00:00')).toBe('07:05');
    expect(formatTime(null, '+02:00')).toBe('–');
  });

  it('parses offsets', () => {
    expect(offsetMs('+02:00')).toBe(7200000);
    expect(offsetMs('-05:30')).toBe(-19800000);
    expect(offsetMs('Z')).toBe(0);
  });

  it('formats lags', () => {
    expect(formatLag(120000)).toBe('2 min');
    expect(formatLag(3 * 3600000 + 600000)).toBe('3 h 10 min');
    expect(formatLag(null)).toBeNull();
  });

  it('shifts dates so local getters read the offset fields', () => {
    const d = shiftedDate(T0 / 1000, '+02:00');
    expect([d.getHours(), d.getMinutes()]).toEqual([9, 5]);
  });
});

describe('clock status', () => {
  const slot = 300000;
  const at = (feedMs, resultsMs, feedIdleSince = null) => ({ feedMs, resultsMs, feedIdleSince, zoneOffset: '+02:00' });

  it('calls a feed within one window of the newest result an open window, at any replay rate', () => {
    const s = clockStatus(at(1000 + slot - 1, 1000), 0, slot, 'live');
    expect(s).toEqual({ key: 'live', text: 'window open', nextEnd: 1000 + slot });
  });

  it('says it is waiting, without a gap in simulated minutes, once the feed is further ahead', () => {
    expect(clockStatus(at(1000 + 13 * slot, 1000), 0, slot, 'live').text).toBe('waiting for the next window');
  });

  it('reports an idle feed, connection states and missing data', () => {
    expect(clockStatus(at(5, 1, 0), 20000, slot, 'live').key).toBe('idle');
    expect(clockStatus(at(5, 1, 15000), 20000, slot, 'live').key).toBe('live');
    expect(clockStatus(at(null, null), 0, slot, 'live').text).toBe('waiting for Kafka');
    expect(clockStatus(at(5, null), 0, slot, 'live').text).toBe('no results yet');
    expect(clockStatus(at(5, 1), 0, slot, 'reconnecting').text).toBe('reconnecting');
  });

  it('formats latency in wall-clock units', () => {
    expect(formatLatency(640)).toBe('640 ms');
    expect(formatLatency(3630)).toBe('3.6 s');
    expect(formatLatency(65000)).toBe('1 min 5 s');
    expect(formatLatency(null)).toBe('–');
  });
});

describe('prices and spreads', () => {
  it('keeps about four significant digits for small values and fewer decimals for large ones', () => {
    expect(formatPrice(0.0110)).toBe('0.0110');
    expect(formatPrice(6.38)).toBe('6.380');
    expect(formatPrice(731.7)).toBe('731.70');
    expect(formatPrice(3906.24)).toBe('3906.2');
    expect(formatPrice(null)).toBe('–');
  });

  it('signs a percentage and shortens it as it grows', () => {
    expect(formatPct(0.0059)).toBe('+0.006%');
    expect(formatPct(-1.234)).toBe('−1.23%');
    expect(formatPct(139.41)).toBe('+139.4%');
    expect(formatPct(null)).toBe('–');
  });
});

