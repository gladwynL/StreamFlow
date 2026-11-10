export type Tone = 'ok' | 'warn' | 'bad' | 'neutral';

const toneClasses: Record<Tone, string> = {
  ok: 'border-ok/40 bg-ok/10 text-ok',
  warn: 'border-warn/40 bg-warn/10 text-warn',
  bad: 'border-bad/40 bg-bad/10 text-bad',
  neutral: 'border-line bg-panel-raised text-ink-muted',
};

const dotClasses: Record<Tone, string> = {
  ok: 'bg-ok',
  warn: 'bg-warn',
  bad: 'bg-bad',
  neutral: 'bg-ink-muted',
};

/** A status label. The text carries the meaning; colour only reinforces it. */
export function StatusPill({ tone, label }: { tone: Tone; label: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${toneClasses[tone]}`}
    >
      <span aria-hidden="true" className={`size-1.5 rounded-full ${dotClasses[tone]}`} />
      {label}
    </span>
  );
}
