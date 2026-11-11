import type { SeriesPoint } from '../api/types';

const MINUTE = 60_000;

/**
 * Adds the minutes the API omits (no activity) between `from` and `to`. Count fields become 0, which is what was
 * measured: no events were processed or dead-lettered in that minute. Latency stays null (nothing to measure), so
 * latency lines show a gap instead of an invented value.
 */
export function fillMinutes(points: SeriesPoint[], from: string, to: string): SeriesPoint[] {
  const byMinute = new Map(points.map((p) => [new Date(p.minute).getTime(), p]));
  const start = Math.ceil(new Date(from).getTime() / MINUTE) * MINUTE;
  const end = Math.floor(new Date(to).getTime() / MINUTE) * MINUTE;
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    return points;
  }
  const filled: SeriesPoint[] = [];
  for (let t = start; t <= end; t += MINUTE) {
    filled.push(
      byMinute.get(t) ?? {
        minute: new Date(t).toISOString(),
        processed: 0,
        processedPerSecond: 0,
        amountCents: 0,
        deadLettered: 0,
        latencySamples: 0,
        p50Millis: null,
        p95Millis: null,
        p99Millis: null,
      },
    );
  }
  return filled;
}
