import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { api } from '../../api/endpoints';
import { EVENT_TYPES, type DeadLetterRow, type LiveSnapshot } from '../../api/types';
import { Modal } from '../../components/Modal';
import {
  EmptyState,
  ErrorState,
  LoadingState,
  PageHeader,
  SyntheticBadge,
} from '../../components/ui';
import { useLive } from '../../live/liveContext';
import { LiveGate } from '../../live/LiveGate';
import { formatAmount, formatCount, formatTime, humanize, shortId } from '../../lib/format';
import { STREAM_LIMIT, mergeStream, toItems, type StreamItem, type StreamStatus } from './stream';

interface StreamState {
  sequence: number;
  items: StreamItem[];
  frozen: StreamItem[] | null;
}

export function LiveStreamPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Live event stream"
        description={
          <>
            The newest processed events and dead letters, pushed every second. At high throughput
            each push carries the 20 newest events, so this view samples the stream; every event is
            in the event store. <SyntheticBadge className="align-middle" />
          </>
        }
      />
      <LiveGate>{(snapshot) => <Stream snapshot={snapshot} />}</LiveGate>
    </div>
  );
}

function Stream({ snapshot }: { snapshot: LiveSnapshot }) {
  const { status } = useLive();
  const [state, setState] = useState<StreamState>(() => ({
    sequence: snapshot.sequence,
    items: mergeStream([], toItems(snapshot)),
    frozen: null,
  }));
  // Fold each new snapshot into the history (adjusting state during render, keyed by sequence).
  if (snapshot.sequence !== state.sequence) {
    setState({
      ...state,
      sequence: snapshot.sequence,
      items: mergeStream(state.items, toItems(snapshot)),
    });
  }
  const [type, setType] = useState('');
  const [statusFilter, setStatusFilter] = useState<'' | StreamStatus>('');
  const [selected, setSelected] = useState<StreamItem | null>(null);

  const paused = state.frozen !== null;
  const shown = (state.frozen ?? state.items).filter(
    (item) =>
      (!type || item.eventType === type || (item.status === 'DEAD_LETTERED' && !type)) &&
      (!statusFilter || item.status === statusFilter),
  );
  const newWhilePaused = paused
    ? state.items.filter((i) => !state.frozen?.some((f) => f.key === i.key)).length
    : 0;

  return (
    <>
      <div className="flex flex-wrap items-end gap-3 rounded-xl border border-line bg-panel p-4">
        <label className="text-sm">
          <span className="text-ink-muted">Event type</span>
          <select value={type} onChange={(e) => setType(e.target.value)} className="field">
            <option value="">All types</option>
            {EVENT_TYPES.map((t) => (
              <option key={t} value={t}>
                {humanize(t)}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="text-ink-muted">Status</span>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as '' | StreamStatus)}
            className="field"
          >
            <option value="">All statuses</option>
            <option value="PROCESSED">Processed</option>
            <option value="REPLAYED">Processed after replay</option>
            <option value="DEAD_LETTERED">Dead-lettered</option>
          </select>
        </label>
        <button
          type="button"
          className={paused ? 'btn-primary' : 'btn-secondary'}
          aria-pressed={paused}
          onClick={() => setState({ ...state, frozen: paused ? null : state.items })}
        >
          {paused ? 'Resume' : 'Pause'}
        </button>
        <p className="ml-auto text-xs text-ink-muted" aria-live="polite">
          {paused
            ? `Paused · ${newWhilePaused} new rows waiting`
            : status === 'live'
              ? `Live · ${formatCount(shown.length)} rows (newest ${STREAM_LIMIT} kept)`
              : 'Not receiving updates'}
        </p>
      </div>

      {shown.length === 0 ? (
        <EmptyState title="No events yet">
          Nothing matches these filters in the recent stream. Start a run on the Simulations page.
        </EmptyState>
      ) : (
        <div className="relative overflow-x-auto rounded-xl border border-line bg-panel">
          <table className="w-full min-w-[720px] text-left text-sm">
            <caption className="sr-only">Recent events and dead letters, newest first</caption>
            <thead className="border-b border-line text-xs text-ink-muted">
              <tr>
                <th scope="col" className="px-4 py-2 font-medium">
                  Time
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Status
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Type
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Event
                </th>
                <th scope="col" className="px-4 py-2 font-medium">
                  Partition · offset
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Latency
                </th>
              </tr>
            </thead>
            <tbody>
              {shown.map((item) => (
                <StreamRow key={item.key} item={item} onOpen={() => setSelected(item)} />
              ))}
            </tbody>
          </table>
        </div>
      )}

      <DetailDialog item={selected} onClose={() => setSelected(null)} />
    </>
  );
}

const statusStyle: Record<StreamStatus, string> = {
  PROCESSED: 'text-ok',
  REPLAYED: 'text-accent',
  DEAD_LETTERED: 'text-bad',
};
const statusLabel: Record<StreamStatus, string> = {
  PROCESSED: 'Processed',
  REPLAYED: 'Replayed',
  DEAD_LETTERED: 'Dead-lettered',
};

function StreamRow({ item, onOpen }: { item: StreamItem; onOpen: () => void }) {
  const e = item.event;
  const d = item.deadLetter;
  return (
    <tr className="border-t border-line/60 hover:bg-panel-raised">
      <td className="tabular px-4 py-2 font-mono text-xs text-ink-muted">{formatTime(item.at)}</td>
      <td className={`px-4 py-2 text-xs font-medium ${statusStyle[item.status]}`}>
        {statusLabel[item.status]}
      </td>
      <td className="px-4 py-2 text-ink">
        {e ? humanize(e.eventType) : humanize(d?.failureCategory ?? '')}
      </td>
      <td className="px-4 py-2">
        <button
          type="button"
          onClick={onOpen}
          className="font-mono text-xs text-accent underline-offset-2 hover:underline"
          aria-label={`Details of ${e ? `event ${e.eventId}` : `dead letter ${d?.id}`}`}
        >
          {e ? shortId(e.eventId) : `#${d?.id}`}
        </button>
      </td>
      <td className="tabular px-4 py-2 font-mono text-xs text-ink-muted">
        {e
          ? `${e.kafkaPartition} · ${e.kafkaOffset}`
          : `${d?.sourcePartition} · ${d?.sourceOffset}`}
      </td>
      <td className="tabular px-4 py-2 text-right font-mono text-xs text-ink">
        {e ? `${formatCount(e.latencyMillis)} ms` : '—'}
      </td>
    </tr>
  );
}

function DetailDialog({ item, onClose }: { item: StreamItem | null; onClose: () => void }) {
  return (
    <Modal open={item !== null} onCancel={onClose} labelledBy="stream-detail-title" wide>
      {item?.event && <EventDetailView eventId={item.event.eventId} onClose={onClose} />}
      {item?.deadLetter && <DeadLetterBrief row={item.deadLetter} onClose={onClose} />}
    </Modal>
  );
}

function EventDetailView({ eventId, onClose }: { eventId: string; onClose: () => void }) {
  const query = useQuery({
    queryKey: ['event', eventId],
    queryFn: ({ signal }) => api.event(eventId, signal),
  });
  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <h2 id="stream-detail-title" className="text-base font-semibold text-ink">
          Event <span className="font-mono text-sm">{eventId}</span>
        </h2>
        <button type="button" className="btn-secondary" onClick={onClose}>
          Close
        </button>
      </div>
      {query.isPending && <LoadingState />}
      {query.isError && <ErrorState message={query.error.message} />}
      {query.data && (
        <>
          <dl className="grid gap-x-6 gap-y-2 text-sm sm:grid-cols-2">
            <Field label="Type" value={humanize(query.data.event.eventType)} />
            <Field
              label="Run"
              value={
                <span className="font-mono">
                  {shortId(query.data.event.runId)} · #{query.data.event.runSeq}
                </span>
              }
            />
            <Field label="Occurred" value={new Date(query.data.event.occurredAt).toISOString()} />
            <Field
              label="Appended to Kafka"
              value={new Date(query.data.event.kafkaTimestamp).toISOString()}
            />
            <Field label="Processed" value={new Date(query.data.event.ingestedAt).toISOString()} />
            <Field label="Latency" value={`${formatCount(query.data.event.latencyMillis)} ms`} />
            <Field
              label="Partition · offset"
              value={`${query.data.event.kafkaPartition} · ${query.data.event.kafkaOffset}`}
            />
            <Field
              label="Amount"
              value={formatAmount(query.data.event.amountCents, query.data.event.currency)}
            />
          </dl>
          {query.data.deadLetters.length > 0 && (
            <p className="text-sm text-ink-muted">
              This event was dead-lettered {query.data.deadLetters.length} time(s) before being
              processed (
              {query.data.deadLetters.map((d) => `#${d.id} ${humanize(d.status)}`).join(', ')}).
            </p>
          )}
          <div>
            <p className="mb-1 text-xs text-ink-muted">Payload (synthetic)</p>
            <pre className="max-h-72 overflow-auto rounded-lg bg-canvas p-3 font-mono text-xs text-ink">
              {JSON.stringify(query.data.payload, null, 2)}
            </pre>
          </div>
        </>
      )}
    </div>
  );
}

function DeadLetterBrief({ row, onClose }: { row: DeadLetterRow; onClose: () => void }) {
  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <h2 id="stream-detail-title" className="text-base font-semibold text-ink">
          Dead letter #{row.id}
        </h2>
        <button type="button" className="btn-secondary" onClick={onClose}>
          Close
        </button>
      </div>
      <dl className="grid gap-x-6 gap-y-2 text-sm sm:grid-cols-2">
        <Field label="Category" value={humanize(row.failureCategory)} />
        <Field label="Status" value={humanize(row.status)} />
        <Field
          label="Source"
          value={`${row.sourceTopic} [${row.sourcePartition}] @ ${row.sourceOffset}`}
        />
        <Field label="Dead-lettered" value={new Date(row.deadLetteredAt).toISOString()} />
        <Field label="Event" value={row.eventId ?? 'none (unparseable record)'} />
      </dl>
      <p className="rounded-lg bg-canvas p-3 font-mono text-xs text-ink">
        {row.errorMessage ?? 'No message'}
      </p>
      <p className="text-sm text-ink-muted">Replay or discard it on the Dead letters page.</p>
    </div>
  );
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-ink-muted">{label}</dt>
      <dd className="truncate text-ink">{value}</dd>
    </div>
  );
}
