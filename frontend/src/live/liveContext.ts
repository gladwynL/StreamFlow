import { createContext, useContext } from 'react';
import type { LiveFeed } from './useLiveFeed';

export const LiveFeedContext = createContext<LiveFeed | null>(null);

/** The shared live feed (see LiveFeedProvider). */
export function useLive(): LiveFeed {
  const feed = useContext(LiveFeedContext);
  if (!feed) {
    throw new Error('useLive must be used inside LiveFeedProvider');
  }
  return feed;
}
