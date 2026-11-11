import type { ReactNode } from 'react';
import type { LiveSnapshot } from '../api/types';
import { LoadingState, Notice } from '../components/ui';
import { formatTime } from '../lib/format';
import { useLive } from './LiveFeedProvider';

/**
 * Renders its children with the latest snapshot. Before the first snapshot it shows a loading or disconnected
 * state; when the feed drops, it keeps showing the last snapshot under a clear "not current" notice.
 */
export function LiveGate({ children }: { children: (snapshot: LiveSnapshot) => ReactNode }) {
  const { snapshot, status, reconnects } = useLive();
  if (!snapshot) {
    return status === 'reconnecting' ? (
      <Notice tone="bad">
        Cannot reach the live feed. Retrying automatically (attempt {reconnects}). Check that the
        api is running.
      </Notice>
    ) : (
      <LoadingState label="Connecting to the live feed…" />
    );
  }
  return (
    <>
      {status === 'reconnecting' && (
        <Notice tone="bad">
          Disconnected from the live feed. Showing data from {formatTime(snapshot.at)}; reconnecting
          automatically.
        </Notice>
      )}
      {status === 'stale' && (
        <Notice tone="warn">
          The live feed is delayed. Showing data from {formatTime(snapshot.at)}.
        </Notice>
      )}
      {children(snapshot)}
    </>
  );
}
