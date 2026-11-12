import type { DeadLetterRow, EventRow, LiveSnapshot } from '../../api/types';

/** Rows kept in this tab. Older rows drop off; the full history is in Analytics and the event API. */
export const STREAM_LIMIT = 500;

export type StreamStatus = 'PROCESSED' | 'REPLAYED' | 'DEAD_LETTERED';

export interface StreamItem {
  key: string;
  at: string;
  status: StreamStatus;
  eventType: string | null;
  event?: EventRow;
  deadLetter?: DeadLetterRow;
}

export function toItems(snapshot: LiveSnapshot): StreamItem[] {
  const events: StreamItem[] = (snapshot.recentEvents ?? []).map((e) => ({
    key: `e:${e.eventId}`,
    at: e.ingestedAt,
    status: e.replayed ? 'REPLAYED' : 'PROCESSED',
    eventType: e.eventType,
    event: e,
  }));
  const dead: StreamItem[] = (snapshot.recentDeadLetters ?? []).map((d) => ({
    key: `d:${d.id}`,
    at: d.deadLetteredAt,
    status: 'DEAD_LETTERED',
    eventType: null,
    deadLetter: d,
  }));
  return [...events, ...dead];
}

/** Merges new items into the bounded history, newest first, without duplicates. */
export function mergeStream(
  history: StreamItem[],
  incoming: StreamItem[],
  limit = STREAM_LIMIT,
): StreamItem[] {
  const seen = new Set(history.map((item) => item.key));
  const fresh = incoming.filter((item) => !seen.has(item.key));
  if (fresh.length === 0) {
    return history;
  }
  return [...fresh, ...history].sort((a, b) => b.at.localeCompare(a.at)).slice(0, limit);
}
