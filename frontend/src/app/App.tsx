import { Link, Route, Routes } from 'react-router';
import { SystemStatusPage } from '../features/system-status/SystemStatusPage';
import { Layout } from './Layout';

export function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<SystemStatusPage />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
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
          Go to system status
        </Link>
      </p>
    </div>
  );
}
