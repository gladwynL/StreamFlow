import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render } from '@testing-library/react';
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router';
import type { LiveSnapshot } from '../api/types';
import { StaticLiveFeed } from '../live/LiveFeedProvider';
import type { LiveFeed } from '../live/useLiveFeed';
import { OperatorKeyProvider } from '../security/OperatorKey';

export function feedOf(snapshot: LiveSnapshot | null, overrides: Partial<LiveFeed> = {}): LiveFeed {
  return {
    snapshot,
    status: snapshot ? 'live' : 'connecting',
    receivedAt: snapshot ? Date.now() : null,
    reconnects: 0,
    trend: [],
    ...overrides,
  };
}

/** The app's providers around `ui`, for render() and rerender() with a changing feed. */
export function appTree(ui: ReactElement, feed: LiveFeed, client: QueryClient, route = '/') {
  return (
    <QueryClientProvider client={client}>
      <OperatorKeyProvider>
        <StaticLiveFeed feed={feed}>
          <MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>
        </StaticLiveFeed>
      </OperatorKeyProvider>
    </QueryClientProvider>
  );
}

export const testQueryClient = () =>
  new QueryClient({ defaultOptions: { queries: { retry: false } } });

/** Renders with the app's providers: no retries, a fixed live feed, an in-memory operator key, a router. */
export function renderApp(
  ui: ReactElement,
  { feed = feedOf(null), route = '/' }: { feed?: LiveFeed; route?: string } = {},
) {
  return render(appTree(ui, feed, testQueryClient(), route));
}
