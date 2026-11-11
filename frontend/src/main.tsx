import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router';
import { App } from './app/App';
import { LiveFeedProvider } from './live/LiveFeedProvider';
import { OperatorKeyProvider } from './security/OperatorKey';
import './index.css';

// One quick retry smooths over a single dropped request without hiding a real outage.
const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1 } } });

const root = document.getElementById('root');
if (!root) {
  throw new Error('Missing #root element');
}

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <OperatorKeyProvider>
        <LiveFeedProvider>
          <BrowserRouter>
            <App />
          </BrowserRouter>
        </LiveFeedProvider>
      </OperatorKeyProvider>
    </QueryClientProvider>
  </StrictMode>,
);
