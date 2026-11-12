import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useId, useState, type FormEvent, type ReactNode } from 'react';
import { api } from '../../api/endpoints';
import { EVENT_TYPES, type ActiveRun, type RunView } from '../../api/types';
import { ConfirmDialog } from '../../components/Modal';
import { StatusPill } from '../../components/StatusPill';
import {
  EmptyState,
  ErrorState,
  LoadingState,
  Meter,
  Notice,
  PageHeader,
  SyntheticBadge,
} from '../../components/ui';
import { useLive } from '../../live/liveContext';
import { formatCount, formatDateTime, formatRate, humanize, shortId } from '../../lib/format';
import { OperatorCancelled, useOperatorAction } from '../../security/useOperatorAction';
import { DEFAULT_FORM, LIMITS, toRequest, type FormState } from './runRequest';

export function SimulationsPage() {
  const { snapshot } = useLive();
  const active = snapshot?.activeRun ?? null;
  const running = active !== null && active.phase !== 'RECONCILED';
  return (
    <div className="space-y-6">
      <PageHeader
        title="Simulations"
        description={
          <>
            Bounded runs of synthetic e-commerce traffic, published to Kafka and processed by the
            workers like any other events. Each run is reconciled end to end when it drains.{' '}
            <SyntheticBadge className="align-middle" />
          </>
        }
      />
      <div className="grid gap-4 lg:grid-cols-5">
        <div className="lg:col-span-2">
          <StartForm disabled={running} />
        </div>
        <div className="lg:col-span-3">
          {active ? (
            <ActiveRunCard run={active} />
          ) : (
            <EmptyState title="No run started from this api yet">
              Configure a run and start it; progress appears here.
            </EmptyState>
          )}
        </div>
      </div>
      <RunHistory />
    </div>
  );
}

function StartForm({ disabled }: { disabled: boolean }) {
  const operator = useOperatorAction();
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(DEFAULT_FORM);
  const [error, setError] = useState<string | null>(null);
  const [started, setStarted] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (patch: Partial<FormState>) => setForm({ ...form, ...patch });

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    setStarted(null);
    const { request, error: problem } = toRequest(form);
    if (!request) {
      setError(problem ?? 'Invalid run');
      return;
    }
    setBusy(true);
    try {
      const result = await operator((key) => api.startRun(key, request));
      if (result) {
        setStarted(
          `Run ${shortId(result.runId)} started: ${formatCount(result.events)} events at ${formatCount(result.ratePerSecond)}/s, seed ${result.seed}.`,
        );
      }
      await queryClient.invalidateQueries({ queryKey: ['runs'] });
    } catch (e) {
      if (!(e instanceof OperatorCancelled)) {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setBusy(false);
    }
  };

  const plannedEvents =
    form.mode === 'count' ? Number(form.events) : Number(form.durationSeconds) * Number(form.rate);
  const plannedSeconds = Number(form.rate) > 0 ? Math.ceil(plannedEvents / Number(form.rate)) : NaN;

  return (
    <form
      onSubmit={(e) => void submit(e)}
      className="space-y-4 rounded-xl border border-line bg-panel p-5"
      aria-labelledby="start-title"
      noValidate
    >
      <h2 id="start-title" className="text-sm font-semibold text-ink">
        Configure a run
      </h2>
      <fieldset>
        <legend className="text-sm text-ink-muted">Length</legend>
        <div className="mt-1 flex gap-4 text-sm">
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="mode"
              checked={form.mode === 'count'}
              onChange={() => set({ mode: 'count' })}
            />
            Event count
          </label>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="mode"
              checked={form.mode === 'duration'}
              onChange={() => set({ mode: 'duration' })}
            />
            Duration
          </label>
        </div>
      </fieldset>
      <div className="grid grid-cols-2 gap-3">
        {form.mode === 'count' ? (
          <NumberField
            label="Events"
            value={form.events}
            onChange={(v) => set({ events: v })}
            hint={`max ${formatCount(LIMITS.events)}`}
          />
        ) : (
          <NumberField
            label="Duration (s)"
            value={form.durationSeconds}
            onChange={(v) => set({ durationSeconds: v })}
            hint={`max ${LIMITS.seconds}`}
          />
        )}
        <NumberField
          label="Rate (events/s)"
          value={form.rate}
          onChange={(v) => set({ rate: v })}
          hint={`max ${formatCount(LIMITS.rate)}`}
        />
        <NumberField
          label="Users"
          value={form.users}
          onChange={(v) => set({ users: v })}
          hint={`max ${formatCount(LIMITS.users)}`}
        />
        <NumberField
          label="Seed"
          value={form.seed}
          onChange={(v) => set({ seed: v })}
          hint="empty = random"
        />
      </div>
      <fieldset>
        <legend className="text-sm text-ink-muted">Event types</legend>
        <div className="mt-1 grid grid-cols-2 gap-1.5 text-sm">
          {EVENT_TYPES.map((type) => (
            <label key={type} className="flex items-center gap-2">
              <input
                type="checkbox"
                className="size-4 accent-[var(--color-accent)]"
                checked={form.types.includes(type)}
                onChange={(e) =>
                  set({
                    types: e.target.checked
                      ? [...form.types, type]
                      : form.types.filter((t) => t !== type),
                  })
                }
              />
              {humanize(type)}
            </label>
          ))}
        </div>
      </fieldset>
      <p className="text-xs text-ink-muted">
        Planned: {Number.isFinite(plannedEvents) ? formatCount(plannedEvents) : '—'} events over{' '}
        {Number.isFinite(plannedSeconds) ? `${plannedSeconds} s` : '—'}. The same seed and settings
        reproduce the same event content.
      </p>
      {error && (
        <p
          role="alert"
          className="rounded-md border border-bad/40 bg-bad/10 px-3 py-2 text-sm text-bad"
        >
          {error}
        </p>
      )}
      {started && (
        <p
          role="status"
          className="rounded-md border border-ok/40 bg-ok/10 px-3 py-2 text-sm text-ok"
        >
          {started}
        </p>
      )}
      <button type="submit" className="btn-primary w-full" disabled={disabled || busy}>
        {disabled ? 'A run is in progress' : busy ? 'Starting…' : 'Start synthetic run'}
      </button>
    </form>
  );
}

function NumberField({
  label,
  value,
  onChange,
  hint,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  hint: string;
}) {
  const id = useId();
  return (
    <div className="text-sm">
      <label htmlFor={id} className="text-ink-muted">
        {label}
      </label>
      <input
        id={id}
        inputMode="numeric"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        aria-describedby={`${id}-hint`}
        className="field tabular font-mono"
      />
      <span id={`${id}-hint`} className="mt-0.5 block text-xs text-ink-faint">
        {hint}
      </span>
    </div>
  );
}

const phaseLabel = {
  PUBLISHING: 'Publishing',
  DRAINING: 'Draining & reconciling',
  RECONCILED: 'Finished',
} as const;

function ActiveRunCard({ run }: { run: ActiveRun }) {
  const operator = useOperatorAction();
  const [confirmStop, setConfirmStop] = useState(false);
  const detail = useQuery({
    queryKey: ['run', run.runId, run.phase],
    queryFn: ({ signal }) => api.run(run.runId ?? '', signal),
    enabled: run.runId !== null && run.phase === 'RECONCILED',
  });

  return (
    <section
      aria-labelledby="active-title"
      className="space-y-4 rounded-xl border border-line bg-panel p-5"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 id="active-title" className="text-sm font-semibold text-ink">
          Run <span className="font-mono">{run.runId ? shortId(run.runId) : '…'}</span>{' '}
          <SyntheticBadge />
        </h2>
        <StatusPill
          tone={run.phase === 'RECONCILED' ? 'neutral' : 'ok'}
          label={phaseLabel[run.phase]}
        />
      </div>
      <Progress
        label="Published (acknowledged by Kafka)"
        value={run.acked}
        max={run.requestedEvents}
      />
      <Progress
        label="Processed by the workers"
        value={run.processed ?? 0}
        max={run.requestedEvents}
        unknown={run.processed === null}
      />
      <dl className="tabular grid grid-cols-2 gap-3 font-mono text-sm sm:grid-cols-4">
        <Stat label="Requested">{formatCount(run.requestedEvents)}</Stat>
        <Stat label="Attempted">{formatCount(run.attempted)}</Stat>
        <Stat label="Publish failures">{formatCount(run.failed)}</Stat>
        <Stat label="Target rate">{formatRate(run.ratePerSecond)}/s</Stat>
      </dl>
      {run.phase === 'PUBLISHING' && (
        <button type="button" className="btn-secondary" onClick={() => setConfirmStop(true)}>
          Stop run
        </button>
      )}
      {run.phase === 'DRAINING' && (
        <p className="text-sm text-ink-muted">
          Publishing is done. Waiting for both consumer groups to reach zero lag, then reconciling.
        </p>
      )}
      {detail.data && (
        <RunResults run={detail.data.run} reasons={detail.data.reconciliation?.reasons ?? []} />
      )}
      <ConfirmDialog
        open={confirmStop}
        title="Stop this run?"
        confirmLabel="Stop publishing"
        tone="danger"
        onConfirm={async () => {
          await operator((key) => api.stopRun(key));
          setConfirmStop(false);
        }}
        onCancel={() => setConfirmStop(false)}
      >
        <p>
          Publishing stops after the current event. Everything already published is still processed
          and the run is reconciled against what was actually published.
        </p>
      </ConfirmDialog>
    </section>
  );
}

function Progress({
  label,
  value,
  max,
  unknown = false,
}: {
  label: string;
  value: number;
  max: number;
  unknown?: boolean;
}) {
  return (
    <div>
      <div className="mb-1 flex justify-between text-xs">
        <span className="text-ink-muted">{label}</span>
        <span className="tabular font-mono text-ink">
          {unknown ? '—' : `${formatCount(value)} / ${formatCount(max)}`}
        </span>
      </div>
      <Meter value={unknown ? 0 : value} max={max} label={label} />
    </div>
  );
}

function Stat({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <dt className="font-sans text-xs text-ink-muted">{label}</dt>
      <dd className="text-ink">{children}</dd>
    </div>
  );
}

/** Publication, processing, duplicates, failures and reconciliation of a finished run, kept separate. */
export function RunResults({ run, reasons }: { run: RunView; reasons: string[] }) {
  const result = run.reconciliationResult;
  return (
    <div className="space-y-3 border-t border-line pt-4">
      <div className="flex items-center gap-2">
        <span className="text-sm text-ink">Reconciliation</span>
        <StatusPill
          tone={result === 'PASS' ? 'ok' : result === null ? 'neutral' : 'bad'}
          label={result ?? 'Not reconciled'}
        />
      </div>
      <dl className="tabular grid grid-cols-2 gap-3 font-mono text-sm sm:grid-cols-3">
        <Stat label="Acknowledged">{formatCount(run.acked)}</Stat>
        <Stat label="Failed (definite)">{formatCount(run.failedDefinite)}</Stat>
        <Stat label="Failed (ambiguous)">{formatCount(run.failedAmbiguous)}</Stat>
        <Stat label="Processed">{formatCount(run.processed)}</Stat>
        <Stat label="Duplicates suppressed">{formatCount(run.duplicatesSuppressed)}</Stat>
        <Stat label="Dead letters">{formatCount(run.deadLetters)}</Stat>
      </dl>
      {reasons.length > 0 && (
        <ul className="list-disc space-y-0.5 pl-5 text-xs text-ink-muted">
          {reasons.map((r) => (
            <li key={r}>{r}</li>
          ))}
        </ul>
      )}
    </div>
  );
}

function RunHistory() {
  const operator = useOperatorAction();
  const queryClient = useQueryClient();
  const runs = useQuery({
    queryKey: ['runs'],
    queryFn: ({ signal }) => api.runs({ limit: 20 }, signal),
    refetchInterval: 10_000,
  });
  const [reconciling, setReconciling] = useState<RunView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  return (
    <section aria-labelledby="history-title" className="space-y-3">
      <h2 id="history-title" className="text-lg font-semibold text-ink">
        Run history
      </h2>
      {notice && (
        <Notice tone="info">
          <span role="status">{notice}</span>
        </Notice>
      )}
      {runs.isPending && <LoadingState />}
      {runs.isError && (
        <ErrorState message={runs.error.message} onRetry={() => void runs.refetch()} />
      )}
      {runs.data?.length === 0 && <EmptyState title="No runs yet" />}
      {runs.data && runs.data.length > 0 && (
        <div className="relative overflow-x-auto rounded-xl border border-line bg-panel">
          <table className="w-full min-w-[960px] text-left text-sm">
            <caption className="sr-only">Simulation runs, newest first (all synthetic)</caption>
            <thead className="border-b border-line text-xs text-ink-muted">
              <tr>
                <th scope="col" className="px-4 py-2 font-medium">
                  Run
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Started
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Config
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Published
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Processed
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Duplicates
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Dead letters
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Reconciliation
                </th>
                <th scope="col" className="px-4 py-2">
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {runs.data.map((r) => (
                <tr key={r.runId} className="border-t border-line/60 hover:bg-panel-raised">
                  <td className="px-4 py-2 font-mono text-ink">{shortId(r.runId)}</td>
                  <td className="px-4 py-2 text-xs text-ink-muted">
                    {formatDateTime(r.startedAt)}
                  </td>
                  <td className="px-4 py-2 text-xs text-ink-muted">
                    {formatCount(r.requestedEvents)} @ {formatCount(r.ratePerSecond)}/s · seed{' '}
                    {r.seed}
                    {r.eventTypes ? ` · ${r.eventTypes.length} types` : ''} · {humanize(r.status)}
                  </td>
                  <td className="tabular px-4 py-2 text-right font-mono text-ink">
                    {r.status === 'RUNNING' ? (
                      <span title="Acknowledged counts are stored when publishing ends; see the run card for live progress">
                        —
                      </span>
                    ) : (
                      formatCount(r.acked)
                    )}
                  </td>
                  <td className="tabular px-4 py-2 text-right font-mono text-ink">
                    {formatCount(r.processed)}
                  </td>
                  <td className="tabular px-4 py-2 text-right font-mono text-ink">
                    {formatCount(r.duplicatesSuppressed)}
                  </td>
                  <td className="tabular px-4 py-2 text-right font-mono text-ink">
                    {formatCount(r.deadLetters)}
                  </td>
                  <td className="px-4 py-2">
                    <StatusPill
                      tone={
                        r.reconciliationResult === 'PASS'
                          ? 'ok'
                          : r.reconciliationResult
                            ? 'bad'
                            : 'neutral'
                      }
                      label={r.reconciliationResult ?? 'Pending'}
                    />
                  </td>
                  <td className="px-4 py-2 text-right">
                    <button
                      type="button"
                      className="btn-secondary px-2.5 py-1 text-xs"
                      onClick={() => setReconciling(r)}
                    >
                      Reconcile
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <ConfirmDialog
        open={reconciling !== null}
        title={`Reconcile run ${reconciling ? shortId(reconciling.runId) : ''} again?`}
        confirmLabel="Reconcile"
        onConfirm={async () => {
          const id = reconciling?.runId ?? '';
          const report = await operator((key) => api.reconcile(key, id));
          setNotice(
            `Run ${shortId(id)} reconciled: ${report?.result ?? 'unknown'}${report && !report.drained ? ' (not drained yet)' : ''}.`,
          );
          setReconciling(null);
          await queryClient.invalidateQueries({ queryKey: ['runs'] });
        }}
        onCancel={() => setReconciling(null)}
      >
        <p>
          Re-checks the run against the event store, ledger and dead letters now, and stores the
          result. It changes no events.
        </p>
      </ConfirmDialog>
    </section>
  );
}
