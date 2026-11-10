import type { ReactNode } from 'react';

interface PanelProps {
  title: string;
  /** Short status element shown next to the title, such as a StatusPill. */
  aside?: ReactNode;
  children: ReactNode;
  className?: string;
}

export function Panel({ title, aside, children, className = '' }: PanelProps) {
  const headingId = `panel-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  return (
    <section
      aria-labelledby={headingId}
      className={`rounded-xl border border-line bg-panel p-5 shadow-sm shadow-black/20 ${className}`}
    >
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 id={headingId} className="text-sm font-semibold tracking-wide text-ink">
          {title}
        </h2>
        {aside}
      </div>
      {children}
    </section>
  );
}

export function KeyValue({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-4 border-t border-line/60 py-2 first:border-t-0 first:pt-0">
      <dt className="text-sm text-ink-muted">{label}</dt>
      <dd className="min-w-0 truncate text-right font-mono text-sm text-ink">{value}</dd>
    </div>
  );
}
