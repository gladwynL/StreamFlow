import type { DeadLetterSummary } from '../../api/types';
import { humanize } from '../../lib/format';

/** Matches the api's replay window (streamflow.retention.replay-window, 7 days by default). */
export const REPLAY_WINDOW_MS = 7 * 24 * 3600_000;

/** Why a dead letter can or cannot be replayed, as the server will decide it. */
export function eligibility(
  d: DeadLetterSummary,
  now = Date.now(),
): { replayable: boolean; reason: string } {
  if (d.status !== 'PENDING') {
    return { replayable: false, reason: `${humanize(d.status)}` };
  }
  if (!d.eventId) {
    return { replayable: false, reason: 'No event id: cannot be deduplicated' };
  }
  if (now - new Date(d.deadLetteredAt).getTime() > REPLAY_WINDOW_MS) {
    return { replayable: false, reason: 'Outside the 7-day replay window' };
  }
  return { replayable: true, reason: 'Replayable' };
}
