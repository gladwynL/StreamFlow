/** "4s ago", "3m ago", "2h ago" relative to `now`; future or invalid times read as "just now". */
export function formatAge(iso: string, now: Date = new Date()): string {
  const seconds = Math.floor((now.getTime() - new Date(iso).getTime()) / 1000);
  if (!Number.isFinite(seconds) || seconds < 1) {
    return 'just now';
  }
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 48) {
    return `${hours}h ago`;
  }
  return `${Math.floor(hours / 24)}d ago`;
}

/** Locale time with seconds, e.g. "2:41:07 PM". */
export function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  });
}

/** Locale date and time, e.g. "Nov 11, 2:41:07 PM". */
export function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  });
}

/** First block of a UUID, enough to tell workers apart at a glance. */
export function shortId(id: string): string {
  return id.split('-')[0] ?? id;
}

const integer = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

/** 12,345 */
export function formatCount(value: number | null | undefined): string | null {
  return value === null || value === undefined ? null : integer.format(value);
}

/** A rate per second with sensible precision: 0.25, 12.4, 3,120. */
export function formatRate(value: number | null | undefined): string | null {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return null;
  }
  if (value === 0) {
    return '0';
  }
  if (value < 1) {
    return value.toFixed(2);
  }
  if (value < 100) {
    return value.toFixed(1);
  }
  return integer.format(value);
}

/** Percentage with one decimal, rounded down so that 99.98% never reads as 100.0%; or null. */
export function formatPercent(value: number | null): string | null {
  return value === null || !Number.isFinite(value)
    ? null
    : (Math.floor(value * 1000) / 10).toFixed(1);
}

/** "≤ 15 ms" — histogram percentiles are bucket upper bounds. */
export function formatLatency(millis: number | null | undefined): string | null {
  if (millis === null || millis === undefined) {
    return null;
  }
  return millis >= 1000
    ? `≤ ${(millis / 1000).toFixed(millis % 1000 === 0 ? 0 : 1)} s`
    : `≤ ${millis} ms`;
}

export function formatAmount(cents: number | null, currency: string | null): string {
  if (cents === null) {
    return '—';
  }
  try {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: currency ?? 'USD',
    }).format(cents / 100);
  } catch {
    return `${(cents / 100).toFixed(2)} ${currency ?? ''}`.trim();
  }
}

/** PURCHASE_COMPLETED → Purchase completed */
export function humanize(constant: string): string {
  const text = constant.toLowerCase().replace(/_/g, ' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}
