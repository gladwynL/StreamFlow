import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import { api } from '../../api/endpoints';
import { DEAD_LETTER_STATUSES, type DeadLetterSummary, type ReplayResult } from '../../api/types';
import { ConfirmDialog, Modal } from '../../components/Modal';
import { StatusPill, type Tone } from '../../components/StatusPill';
import { EmptyState, ErrorState, LoadingState, Notice, PageHeader } from '../../components/ui';
import { useLive } from '../../live/LiveFeedProvider';
import { formatAge, formatCount, formatDateTime, humanize, shortId } from '../../lib/format';
import { useOperatorAction } from '../../security/useOperatorAction';

export const PAGE_SIZE = 25;
/** Matches the api's replay window (streamflow.retention.replay-window, 7 days by default). */
export const REPLAY_WINDOW_MS = 7 * 24 * 3600_000;
const MAX_REASON = 200;

/** Why a dead letter can or cannot be replayed, as the server will decide it. */
export function eligibility(
  d: DeadLetterSummary,
  now = Date.now(),
): { replayable: boolean; reason: string } {
  if (d.status !== 'PENDING') {
    return { replayable: false, reason: `${humanize(d.status)}` };
  }
  if (!d.eventId) {
    return { replayable: false, reason: 'No event id: cannot be deduplicated' };
  }
  if (now - new Date(d.deadLetteredAt).getTime() > REPLAY_WINDOW_MS) {
    return { replayable: false, reason: 'Outside the 7-day replay window' };
  }
  return { replayable: true, reason: 'Replayable' };
}

const statusTone: Record<string, Tone> = {
  PENDING: 'warn',
  REPLAY_REQUESTED: 'neutral',
  REPLAYED: 'ok',
  DISCARDED: 'neutral',
  EXPIRED: 'bad',
};

export function DeadLettersPage() {
  const queryClient = useQueryClient();
  const operator = useOperatorAction();
  const { snapshot } = useLive();
  const [status, setStatus] = useState('PENDING');
  const [cursors, setCursors] = useState<(number | null)[]>([null]);
  const before = cursors[cursors.length - 1] ?? null;
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [confirmReplay, setConfirmReplay] = useState(false);
  const [discarding, setDiscarding] = useState<DeadLetterSummary | null>(null);
  const [reason, setReason] = useState('');
  const [details, setDetails] = useState<number | null>(null);
  const [results, setResults] = useState<ReplayResult[] | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const list = useQuery({
    queryKey: ['dead-letters', status, before],
    queryFn: ({ signal }) => api.deadLetters({ status, before, limit: PAGE_SIZE + 1 }, signal),
    refetchInterval: 10_000,
  });
  const rows = (list.data ?? []).slice(0, PAGE_SIZE);
  const hasMore = (list.data?.length ?? 0) > PAGE_SIZE;

  const changeStatus = (value: string) => {
    setStatus(value);
    setCursors([null]);
    setSelected(new Set());
  };
  const toggle = (id: number) => {
    const next = new Set(selected);
    if (next.has(id)) {
      next.delete(id);
    } else {
      next.add(id);
    }
    setSelected(next);
  };
  const refresh = () => queryClient.invalidateQueries({ queryKey: ['dead-letters'] });

  const replay = async () => {
    const ids = [...selected];
    const outcome = await operator((key) => api.replay(key, ids));
    setResults(outcome ?? []);
    setSelected(new Set());
    setConfirmReplay(false);
    await refresh();
  };
  const discard = async () => {
    if (!discarding) {
      return;
    }
    const id = discarding.id;
    await operator((key) => api.discard(key, id, reason.trim()));
    setMessage(`Dead letter #${id} was discarded. It will not be replayed.`);
    setDiscarding(null);
    setReason('');
    await refresh();
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Dead letters"
        description="Records that could not be processed, stored with their provenance and failure. Replay republishes the original event (deduplicated by event id); discard closes it permanently. Both require the operator key."
      />

      {snapshot?.deadLetters && (
        <ul aria-label="Dead letters by status" className="flex flex-wrap gap-2">
          {DEAD_LETTER_STATUSES.map((s) => (
            <li key={s} className="rounded-lg border border-line bg-panel px-3 py-1.5 text-sm">
              <span className="text-ink-muted">{humanize(s)}</span>{' '}
              <span className="tabular font-mono text-ink">
                {formatCount(snapshot.deadLetters?.[s] ?? 0)}
              </span>
            </li>
          ))}
        </ul>
      )}

      {message && (
        <Notice tone="info">
          <div className="flex items-start justify-between gap-3">
            <span role="status">{message}</span>
            <button
              type="button"
              className="text-xs text-ink-muted hover:text-ink"
              onClick={() => setMessage(null)}
            >
              Dismiss
            </button>
          </div>
        </Notice>
      )}
      {results && <ReplayResults results={results} onDismiss={() => setResults(null)} />}

      <div className="flex flex-wrap items-end gap-3 rounded-xl border border-line bg-panel p-4">
        <label className="text-sm">
          <span className="text-ink-muted">Status</span>
          <select value={status} onChange={(e) => changeStatus(e.target.value)} className="field">
            <option value="">All statuses</option>
            {DEAD_LETTER_STATUSES.map((s) => (
              <option key={s} value={s}>
                {humanize(s)}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="btn-primary ml-auto"
          disabled={selected.size === 0}
          onClick={() => setConfirmReplay(true)}
        >
          Replay selected ({selected.size})
        </button>
      </div>

      {list.isPending && <LoadingState />}
      {list.isError && (
        <ErrorState message={list.error.message} onRetry={() => void list.refetch()} />
      )}
      {list.data && rows.length === 0 && (
        <EmptyState
          title={status ? `No ${humanize(status).toLowerCase()} dead letters` : 'No dead letters'}
        >
          Records land here when processing fails permanently. Fault scenarios can be enabled on a
          worker to exercise this path.
        </EmptyState>
      )}
      {rows.length > 0 && (
        <div className="relative overflow-x-auto rounded-xl border border-line bg-panel">
          <table className="w-full min-w-[900px] text-left text-sm">
            <caption className="sr-only">Dead letters, newest first</caption>
            <thead className="border-b border-line text-xs text-ink-muted">
              <tr>
                <th scope="col" className="w-10 px-4 py-2">
                  <span className="sr-only">Select</span>
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Id
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Category
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Status
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Provenance
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Dead-lettered
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Eligibility
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((d) => {
                const e = eligibility(d);
                const canDiscard = d.status === 'PENDING' || d.status === 'EXPIRED';
                return (
                  <tr
                    key={d.id}
                    className="border-t border-line/60 align-top hover:bg-panel-raised"
                  >
                    <td className="px-4 py-2.5">
                      <input
                        type="checkbox"
                        className="size-4 accent-[var(--color-accent)]"
                        aria-label={`Select dead letter ${d.id}`}
                        disabled={!e.replayable}
                        checked={selected.has(d.id)}
                        onChange={() => toggle(d.id)}
                      />
                    </td>
                    <td className="px-4 py-2.5 font-mono text-ink">#{d.id}</td>
                    <td className="px-4 py-2.5">
                      <p className="text-ink">{humanize(d.failureCategory)}</p>
                      <p
                        className="max-w-[18rem] truncate text-xs text-ink-muted"
                        title={d.errorMessage ?? ''}
                      >
                        {d.errorMessage ?? '—'}
                      </p>
                    </td>
                    <td className="px-4 py-2.5">
                      <StatusPill
                        tone={statusTone[d.status] ?? 'neutral'}
                        label={humanize(d.status)}
                      />
                      {d.replayCount > 0 && (
                        <p className="mt-1 text-xs text-ink-muted">replayed {d.replayCount}×</p>
                      )}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-ink-muted">
                      <p>
                        {d.sourceTopic} [{d.sourcePartition}] @ {d.sourceOffset}
                      </p>
                      <p>{d.runId ? `run ${shortId(d.runId)} #${d.runSeq}` : 'no run'}</p>
                    </td>
                    <td
                      className="px-4 py-2.5 text-xs text-ink-muted"
                      title={formatDateTime(d.deadLetteredAt)}
                    >
                      {formatAge(d.deadLetteredAt)}
                    </td>
                    <td
                      className={`px-4 py-2.5 text-xs ${e.replayable ? 'text-ok' : 'text-ink-muted'}`}
                    >
                      {e.reason}
                    </td>
                    <td className="px-4 py-2.5 text-right whitespace-nowrap">
                      <button
                        type="button"
                        className="btn-secondary px-2.5 py-1 text-xs"
                        onClick={() => setDetails(d.id)}
                      >
                        Details
                      </button>{' '}
                      {canDiscard && (
                        <button
                          type="button"
                          className="btn-secondary px-2.5 py-1 text-xs text-bad"
                          onClick={() => setDiscarding(d)}
                          aria-label={`Discard dead letter ${d.id}`}
                        >
                          Discard
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <nav aria-label="Dead letter pages" className="flex items-center justify-between">
        <button
          type="button"
          className="btn-secondary"
          disabled={cursors.length === 1}
          onClick={() => setCursors(cursors.slice(0, -1))}
        >
          Newer
        </button>
        <span className="text-xs text-ink-muted">Page {cursors.length}</span>
        <button
          type="button"
          className="btn-secondary"
          disabled={!hasMore}
          onClick={() => setCursors([...cursors, rows[rows.length - 1]?.id ?? null])}
        >
          Older
        </button>
      </nav>

      <ConfirmDialog
        open={confirmReplay}
        title={`Replay ${selected.size} dead letter(s)?`}
        confirmLabel="Replay"
        onConfirm={replay}
        onCancel={() => setConfirmReplay(false)}
      >
        <p>
          Each selected record is republished to the event topic with its original event id. If an
          event was already processed, the ledger suppresses it as a duplicate, so it is applied at
          most once.
        </p>
        <p className="font-mono text-xs">{[...selected].map((id) => `#${id}`).join(', ')}</p>
      </ConfirmDialog>

      <ConfirmDialog
        open={discarding !== null}
        title={`Discard dead letter #${discarding?.id ?? ''}?`}
        confirmLabel="Discard permanently"
        tone="danger"
        confirmDisabled={reason.trim().length === 0}
        onConfirm={discard}
        onCancel={() => {
          setDiscarding(null);
          setReason('');
        }}
      >
        <p>
          Discarding is final: the record will never be replayed. It stays visible here as
          Discarded.
        </p>
        <label className="block">
          <span className="text-ink">Reason (required)</span>
          <textarea
            value={reason}
            maxLength={MAX_REASON}
            onChange={(e) => setReason(e.target.value)}
            rows={3}
            className="field"
          />
        </label>
      </ConfirmDialog>

      <DetailsDialog id={details} onClose={() => setDetails(null)} />
    </div>
  );
}

const outcomeText: Record<string, string> = {
  REPLAYED: 'Replayed',
  NOT_FOUND: 'Not found',
  NOT_PENDING: 'Not pending (already handled)',
  OUTSIDE_REPLAY_WINDOW: 'Outside the replay window',
  NO_EVENT_ID: 'No event id',
};

function ReplayResults({ results, onDismiss }: { results: ReplayResult[]; onDismiss: () => void }) {
  const replayed = results.filter((r) => r.outcome === 'REPLAYED').length;
  return (
    <div role="status" className="rounded-lg border border-accent/30 bg-accent/5 px-4 py-3 text-sm">
      <div className="flex items-start justify-between gap-3">
        <p className="text-ink">
          Replay finished: {replayed} of {results.length} republished.
        </p>
        <button type="button" className="text-xs text-ink-muted hover:text-ink" onClick={onDismiss}>
          Dismiss
        </button>
      </div>
      <ul className="mt-2 space-y-0.5 font-mono text-xs text-ink-muted">
        {results.map((r) => (
          <li key={r.id}>
            #{r.id}: {outcomeText[r.outcome] ?? r.outcome}
            {r.detail ? ` (${r.detail})` : ''}
          </li>
        ))}
      </ul>
    </div>
  );
}

function DetailsDialog({ id, onClose }: { id: number | null; onClose: () => void }) {
  const query = useQuery({
    queryKey: ['dead-letter', id],
    queryFn: ({ signal }) => api.deadLetter(id ?? 0, signal),
    enabled: id !== null,
  });
  const d = query.data;
  return (
    <Modal open={id !== null} onCancel={onClose} labelledBy="dl-detail-title" wide>
      <div className="space-y-4">
        <div className="flex items-start justify-between gap-4">
          <h2 id="dl-detail-title" className="text-base font-semibold text-ink">
            Dead letter #{id}
          </h2>
          <button type="button" className="btn-secondary" onClick={onClose}>
            Close
          </button>
        </div>
        {query.isPending && <LoadingState />}
        {query.isError && <ErrorState message={query.error.message} />}
        {d && (
          <>
            <dl className="grid gap-x-6 gap-y-2 text-sm sm:grid-cols-2">
              <Item label="Status">
                {humanize(d.summary.status)}
                {d.statusChangedAt ? ` · ${formatDateTime(d.statusChangedAt)}` : ''}
              </Item>
              <Item label="Category">{humanize(d.summary.failureCategory)}</Item>
              <Item label="Exception">{d.exceptionClass ?? '—'}</Item>
              <Item label="Attempts">{d.attempts ?? '—'}</Item>
              <Item label="Source">{`${d.summary.sourceTopic} [${d.summary.sourcePartition}] @ ${d.summary.sourceOffset}`}</Item>
              <Item label="Dead-lettered">{formatDateTime(d.summary.deadLetteredAt)}</Item>
              <Item label="Event id">{d.summary.eventId ?? 'none'}</Item>
              <Item label="Run">
                {d.summary.runId ? `${d.summary.runId} #${d.summary.runSeq}` : '—'}
              </Item>
              <Item label="Replays">{`${d.summary.replayCount}${d.replayedAt ? ` · last ${formatDateTime(d.replayedAt)}` : ''}`}</Item>
              {d.discardReason && <Item label="Discard reason">{d.discardReason}</Item>}
            </dl>
            <p className="rounded-lg bg-canvas p-3 font-mono text-xs break-words text-ink">
              {d.summary.errorMessage ?? 'No message'}
            </p>
            <div>
              <p className="mb-1 text-xs text-ink-muted">
                Payload ({formatCount(d.payloadBytes)} bytes, first 4 KiB)
              </p>
              <pre className="max-h-60 overflow-auto rounded-lg bg-canvas p-3 font-mono text-xs break-all whitespace-pre-wrap text-ink">
                {d.payloadText ?? `base64:${d.payloadBase64 ?? ''}`}
              </pre>
            </div>
            <div>
              <p className="mb-1 text-xs text-ink-muted">Kafka headers</p>
              <pre className="max-h-40 overflow-auto rounded-lg bg-canvas p-3 font-mono text-xs text-ink">
                {JSON.stringify(d.headers, null, 2)}
              </pre>
            </div>
          </>
        )}
      </div>
    </Modal>
  );
}

function Item({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-ink-muted">{label}</dt>
      <dd className="break-words text-ink">{children}</dd>
    </div>
  );
}
