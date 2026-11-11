import { useEffect, useState, type ReactNode } from 'react';
import { NavLink } from 'react-router';
import { useLive } from '../live/LiveFeedProvider';
import type { FeedStatus } from '../live/useLiveFeed';
import { useOperator } from '../security/OperatorKey';

export const NAV = [
  { to: '/', label: 'Overview', end: true },
  { to: '/live', label: 'Live stream' },
  { to: '/kafka', label: 'Kafka & processing' },
  { to: '/analytics', label: 'Analytics' },
  { to: '/dead-letters', label: 'Dead letters' },
  { to: '/simulations', label: 'Simulations' },
  { to: '/system', label: 'System' },
];

export function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-screen flex-col">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:top-3 focus:left-3 focus:z-20 focus:rounded-md focus:bg-panel-raised focus:px-3 focus:py-2 focus:text-sm"
      >
        Skip to content
      </a>
      <header className="sticky top-0 z-10 border-b border-line bg-canvas/85 backdrop-blur">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-x-6 gap-y-2 px-4 py-3 sm:px-6">
          <div className="flex items-center gap-3">
            <Logo />
            <span className="text-base font-semibold tracking-tight text-ink">StreamFlow</span>
            <span className="rounded border border-line px-1.5 py-0.5 text-[11px] text-ink-muted">
              local
            </span>
          </div>
          <div className="flex items-center gap-2">
            <ConnectionBadge />
            <OperatorBadge />
          </div>
        </div>
        <nav aria-label="Primary" className="mx-auto max-w-7xl overflow-x-auto px-2 sm:px-4">
          <ul className="flex min-w-max gap-1 pb-2">
            {NAV.map((item) => (
              <li key={item.to}>
                <NavLink
                  to={item.to}
                  end={item.end}
                  className={({ isActive }) =>
                    `block rounded-md px-3 py-1.5 text-sm whitespace-nowrap ${
                      isActive
                        ? 'bg-panel-raised font-medium text-ink shadow-[inset_0_-2px_0_var(--color-accent)]'
                        : 'text-ink-muted hover:bg-panel hover:text-ink'
                    }`
                  }
                >
                  {item.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>
      </header>
      <main id="main" tabIndex={-1} className="mx-auto w-full max-w-7xl flex-1 px-4 py-8 sm:px-6">
        {children}
      </main>
      <footer className="border-t border-line">
        <p className="mx-auto max-w-7xl px-4 py-4 text-xs text-ink-muted sm:px-6">
          Every value shown is measured by the StreamFlow backend from Kafka and PostgreSQL. Events
          come from the built-in simulator and are synthetic. A dash (—) means a value could not be
          measured.
        </p>
      </footer>
    </div>
  );
}

function Logo() {
  return (
    <svg aria-hidden="true" viewBox="0 0 32 32" className="size-7">
      <rect width="32" height="32" rx="8" fill="#191d26" />
      <path
        d="M5 11h9l4 10h9"
        fill="none"
        stroke="#5eead4"
        strokeWidth="2.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path d="M5 21h6" fill="none" stroke="#3987e5" strokeWidth="2.6" strokeLinecap="round" />
      <circle cx="24" cy="11" r="2.4" fill="#c4b5fd" />
    </svg>
  );
}

const statusText: Record<FeedStatus, string> = {
  connecting: 'Connecting',
  live: 'Live',
  stale: 'Delayed',
  reconnecting: 'Reconnecting',
};

export function ConnectionBadge() {
  const { status, receivedAt } = useLive();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  const age = receivedAt === null ? null : Math.max(0, Math.round((now - receivedAt) / 1000));
  const tone =
    status === 'live'
      ? 'border-ok/40 text-ok'
      : status === 'stale'
        ? 'border-warn/40 text-warn'
        : status === 'reconnecting'
          ? 'border-bad/40 text-bad'
          : 'border-line text-ink-muted';
  return (
    <span
      role="status"
      aria-live="polite"
      className={`inline-flex items-center gap-2 rounded-full border bg-panel px-2.5 py-1 text-xs font-medium ${tone}`}
      title="Live feed: one snapshot per second over WebSocket"
    >
      <span
        aria-hidden="true"
        className={`size-1.5 rounded-full ${status === 'live' ? 'live-dot bg-ok' : 'bg-current'}`}
      />
      {statusText[status]}
      {status !== 'live' && age !== null && (
        <span className="text-ink-faint">· last data {age}s ago</span>
      )}
    </span>
  );
}

function OperatorBadge() {
  const { unlocked, lock, requireKey } = useOperator();
  return unlocked ? (
    <button
      type="button"
      onClick={lock}
      className="inline-flex items-center gap-1.5 rounded-full border border-accent/40 bg-accent/10 px-2.5 py-1 text-xs font-medium text-accent"
    >
      Operator unlocked · Lock
    </button>
  ) : (
    <button
      type="button"
      onClick={() => void requireKey()}
      className="inline-flex items-center gap-1.5 rounded-full border border-line bg-panel px-2.5 py-1 text-xs font-medium text-ink-muted hover:text-ink"
    >
      Read-only · Unlock
    </button>
  );
}
