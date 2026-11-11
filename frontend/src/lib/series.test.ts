import type { SeriesPoint } from '../api/types';
import { fillMinutes } from './series';

const point = (minute: string, processed: number): SeriesPoint => ({
  minute,
  processed,
  processedPerSecond: processed / 60,
  amountCents: 0,
  deadLettered: 0,
  latencySamples: processed,
  p50Millis: 10,
  p95Millis: 20,
  p99Millis: 30,
});

describe('fillMinutes', () => {
  it('fills idle minutes with zero counts and null latency, keeping measured minutes as they are', () => {
    const measured = [point('2026-11-11T10:01:00Z', 600), point('2026-11-11T10:03:00Z', 120)];
    const filled = fillMinutes(measured, '2026-11-11T10:00:30Z', '2026-11-11T10:03:40Z');
    expect(filled.map((p) => [p.minute, p.processed, p.p50Millis])).toEqual([
      ['2026-11-11T10:01:00Z', 600, 10],
      ['2026-11-11T10:02:00.000Z', 0, null],
      ['2026-11-11T10:03:00Z', 120, 10],
    ]);
  });
});
