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
  return `${Math.floor(minutes / 60)}h ago`;
}

/** Locale time with seconds, e.g. "2:41:07 PM". */
export function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  });
}

/** First block of a UUID, enough to tell workers apart at a glance. */
export function shortId(id: string): string {
  return id.split('-')[0] ?? id;
}
