import { lazy, Suspense } from 'react';
import { Link, Route, Routes } from 'react-router';
import { LoadingState } from '../components/ui';
import { Layout } from './Layout';

// Sections load on demand so the first paint does not wait for the charting library.
const OverviewPage = lazy(() =>
  import('../features/overview/OverviewPage').then((m) => ({ default: m.OverviewPage })),
);
const LiveStreamPage = lazy(() =>
  import('../features/live-stream/LiveStreamPage').then((m) => ({ default: m.LiveStreamPage })),
);
const KafkaPage = lazy(() =>
  import('../features/kafka/KafkaPage').then((m) => ({ default: m.KafkaPage })),
);
const AnalyticsPage = lazy(() =>
  import('../features/analytics/AnalyticsPage').then((m) => ({ default: m.AnalyticsPage })),
);
const DeadLettersPage = lazy(() =>
  import('../features/dead-letters/DeadLettersPage').then((m) => ({ default: m.DeadLettersPage })),
);
const SimulationsPage = lazy(() =>
  import('../features/simulations/SimulationsPage').then((m) => ({ default: m.SimulationsPage })),
);
const SystemStatusPage = lazy(() =>
  import('../features/system-status/SystemStatusPage').then((m) => ({
    default: m.SystemStatusPage,
  })),
);

export function App() {
  return (
    <Layout>
      <Suspense fallback={<LoadingState />}>
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/live" element={<LiveStreamPage />} />
          <Route path="/kafka" element={<KafkaPage />} />
          <Route path="/analytics" element={<AnalyticsPage />} />
          <Route path="/dead-letters" element={<DeadLettersPage />} />
          <Route path="/simulations" element={<SimulationsPage />} />
          <Route path="/system" element={<SystemStatusPage />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </Suspense>
    </Layout>
  );
}

function NotFound() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-semibold text-ink">Page not found</h1>
      <p className="text-sm text-ink-muted">
        This page does not exist.{' '}
        <Link to="/" className="text-accent underline underline-offset-2">
          Go to the overview
        </Link>
      </p>
    </div>
  );
}
