import type { ReactNode } from 'react';
import { NavLink } from 'react-router';

export function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-screen flex-col">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:top-3 focus:left-3 focus:z-10 focus:rounded-md focus:bg-panel-raised focus:px-3 focus:py-2 focus:text-sm"
      >
        Skip to content
      </a>
      <header className="border-b border-line bg-panel/80">
        <div className="mx-auto flex max-w-6xl items-center justify-between gap-6 px-4 py-3 sm:px-6">
          <div className="flex items-center gap-3">
            <svg aria-hidden="true" viewBox="0 0 32 32" className="size-7">
              <rect width="32" height="32" rx="7" fill="#15213a" />
              <path
                d="M6 21c4 0 4-10 8-10s4 10 8 10 4-6 4-6"
                fill="none"
                stroke="#38bdf8"
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            <span className="text-base font-semibold tracking-tight text-ink">StreamFlow</span>
            <span className="rounded border border-line px-1.5 py-0.5 text-[11px] text-ink-muted">
              local
            </span>
          </div>
          <nav aria-label="Primary">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                `rounded-md px-3 py-1.5 text-sm ${isActive ? 'bg-panel-raised text-ink' : 'text-ink-muted hover:text-ink'}`
              }
            >
              System status
            </NavLink>
          </nav>
        </div>
      </header>
      <main id="main" className="mx-auto w-full max-w-6xl flex-1 px-4 py-8 sm:px-6">
        {children}
      </main>
      <footer className="border-t border-line">
        <p className="mx-auto max-w-6xl px-4 py-4 text-xs text-ink-muted sm:px-6">
          Every value on this page comes from a live backend check. Nothing is simulated or cached.
        </p>
      </footer>
    </div>
  );
}
