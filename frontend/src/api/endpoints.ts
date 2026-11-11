import { getJson, getOptionalJson, postOperator } from './client';
import type {
  ConsumerGroups,
  DeadLetterDetail,
  DeadLetterSummary,
  Distribution,
  EventDetail,
  EventPage,
  LiveSnapshot,
  Reconciliation,
  ReplayResult,
  RunDetail,
  RunProgress,
  RunView,
  Series,
  StartRequest,
  Started,
} from './types';

export type Range = '15m' | '1h' | '6h' | '24h';
export const RANGES: Range[] = ['15m', '1h', '6h', '24h'];

function query(params: Record<string, string | number | null | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== '') {
      search.set(key, String(value));
    }
  }
  const text = search.toString();
  return text ? `?${text}` : '';
}

export const api = {
  live: (signal?: AbortSignal) => getJson<LiveSnapshot>('/api/v1/live', signal),
  events: (
    params: { type?: string; runId?: string; cursor?: string | null; limit?: number },
    signal?: AbortSignal,
  ) => getJson<EventPage>(`/api/v1/events${query(params)}`, signal),
  event: (id: string, signal?: AbortSignal) =>
    getJson<EventDetail>(`/api/v1/events/${encodeURIComponent(id)}`, signal),
  series: (range: Range, signal?: AbortSignal) =>
    getJson<Series>(`/api/v1/analytics/series${query({ range })}`, signal),
  distribution: (params: { range?: Range; runId?: string }, signal?: AbortSignal) =>
    getJson<Distribution>(`/api/v1/analytics/distribution${query(params)}`, signal),
  consumerGroups: (signal?: AbortSignal) =>
    getJson<ConsumerGroups>('/api/v1/kafka/consumer-groups', signal),
  runs: (params: { before?: string; limit?: number }, signal?: AbortSignal) =>
    getJson<RunView[]>(`/api/v1/simulations${query(params)}`, signal),
  run: (id: string, signal?: AbortSignal) =>
    getJson<RunDetail>(`/api/v1/simulations/${encodeURIComponent(id)}`, signal),
  activeRun: (signal?: AbortSignal) =>
    getOptionalJson<RunProgress>('/api/v1/simulations/active', signal),
  deadLetters: (
    params: { status?: string; before?: number | null; limit?: number },
    signal?: AbortSignal,
  ) => getJson<DeadLetterSummary[]>(`/api/v1/dead-letters${query(params)}`, signal),
  deadLetter: (id: number, signal?: AbortSignal) =>
    getJson<DeadLetterDetail>(`/api/v1/dead-letters/${id}`, signal),

  startRun: (key: string, request: StartRequest) =>
    postOperator<Started>('/api/v1/simulations', key, request),
  stopRun: (key: string) => postOperator<null>('/api/v1/simulations/active/stop', key),
  reconcile: (key: string, runId: string) =>
    postOperator<Reconciliation>(`/api/v1/simulations/${encodeURIComponent(runId)}/reconcile`, key),
  replay: (key: string, ids: number[]) =>
    postOperator<ReplayResult[]>('/api/v1/dead-letters/replay', key, { ids }),
  discard: (key: string, id: number, reason: string) =>
    postOperator<{ id: number; status: string }>(`/api/v1/dead-letters/${id}/discard`, key, {
      reason,
    }),
};
