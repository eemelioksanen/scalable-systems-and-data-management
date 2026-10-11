import { describe, expect, it } from 'vitest';
import { formatDateTime, formatLag, formatTime, offsetMs, shiftedDate } from './format.js';

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
