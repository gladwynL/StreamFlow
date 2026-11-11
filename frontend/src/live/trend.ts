import type { LiveSnapshot } from '../api/types';

/** One point of the in-browser trend: values copied from a backend snapshot, nothing derived. */
export interface TrendPoint {
  at: string;
  sequence: number;
  published: number | null;
  processed: number;
  lag: number | null;
  [key: string]: unknown;
}

/** Snapshots kept for the "since this page opened" trend: five minutes at one per second. */
export const TREND_POINTS = 300;

export function appendTrend(trend: TrendPoint[], snapshot: LiveSnapshot): TrendPoint[] {
  const last = trend[trend.length - 1];
  if (last && snapshot.sequence <= last.sequence) {
    return trend;
  }
  const point: TrendPoint = {
    at: snapshot.at,
    sequence: snapshot.sequence,
    published: snapshot.throughput?.publishedPerSecond ?? null,
    processed: snapshot.throughput?.processedPerSecond ?? 0,
    lag: snapshot.kafka.totalLag,
  };
  const next =
    trend.length >= TREND_POINTS ? trend.slice(trend.length - TREND_POINTS + 1) : trend.slice();
  next.push(point);
  return next;
}
