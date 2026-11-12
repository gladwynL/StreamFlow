import type { LiveSnapshot } from '../../api/types';

/**
 * Of the records that reached a final outcome in the last minute (applied or dead-lettered), the share that was
 * applied. Null when nothing finished.
 */
export function appliedShare(snapshot: LiveSnapshot): number | null {
  const t = snapshot.throughput;
  if (!t) {
    return null;
  }
  const finished = t.processedLastMinute + t.deadLetteredLastMinute;
  return finished === 0 ? null : t.processedLastMinute / finished;
}
