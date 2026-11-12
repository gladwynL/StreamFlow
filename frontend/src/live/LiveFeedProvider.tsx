import type { ReactNode } from 'react';
import { LiveFeedContext } from './liveContext';
import { api } from '../api/endpoints';
import { useLiveFeed, type LiveFeed } from './useLiveFeed';

const resync = (signal: AbortSignal) => api.live(signal);

/** One live connection shared by every section of the dashboard. */
export function LiveFeedProvider({ children }: { children: ReactNode }) {
  const feed = useLiveFeed({ resync });
  return <LiveFeedContext.Provider value={feed}>{children}</LiveFeedContext.Provider>;
}

/** For tests: provides a fixed feed without a connection. */
export function StaticLiveFeed({ feed, children }: { feed: LiveFeed; children: ReactNode }) {
  return <LiveFeedContext.Provider value={feed}>{children}</LiveFeedContext.Provider>;
}
