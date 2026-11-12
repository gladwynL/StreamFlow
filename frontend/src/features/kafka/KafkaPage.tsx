import { useQuery } from '@tanstack/react-query';
import { api } from '../../api/endpoints';
import type { GroupState, LiveSnapshot } from '../../api/types';
import { ErrorState, LoadingState, Metric, Notice, PageHeader } from '../../components/ui';
import { StatusPill } from '../../components/StatusPill';
import { LiveGate } from '../../live/LiveGate';
import { formatAge, formatCount, formatRate, humanize, shortId } from '../../lib/format';

export function KafkaPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Kafka & processing"
        description="Partition ownership and lag as the Kafka group coordinator reports them, worker health from heartbeats, and retry and dead-letter activity."
      />
      <LiveGate>{(snapshot) => <KafkaBody snapshot={snapshot} />}</LiveGate>
      <IngestorGroup />
    </div>
  );
}

function KafkaBody({ snapshot }: { snapshot: LiveSnapshot }) {
  const k = snapshot.kafka;
  const failed = snapshot.throughput?.failedAttemptsPerSecond;
  const maxLag = Math.max(1, ...(k.partitions ?? []).map((p) => p.lag));
  return (
    <>
      {k.error && <Notice tone="bad">Kafka could not be queried: {k.error}</Notice>}
      <section aria-label="Consumer group" className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Metric
          label="Group state"
          value={k.groupState ? humanize(k.groupState) : null}
          hint="Processing consumer group"
        />
        <Metric
          label="Members"
          value={formatCount(k.members)}
          unit="consumers"
          hint="Assigned by the group coordinator"
        />
        <Metric
          label="Total lag"
          value={formatCount(k.totalLag)}
          unit="records"
          hint="Log-end minus committed offset"
        />
        <Metric
          label="Records in topic"
          value={formatCount(k.logEndOffset)}
          hint="Sum of log-end offsets"
        />
      </section>

      <section
        aria-labelledby="partitions-title"
        className="rounded-xl border border-line bg-panel"
      >
        <h2
          id="partitions-title"
          className="border-b border-line px-5 py-3 text-sm font-semibold text-ink"
        >
          Partitions of the event topic
        </h2>
        {!k.partitions ? (
          <p className="px-5 py-4 text-sm text-ink-muted">
            Unavailable while Kafka cannot be queried.
          </p>
        ) : (
          <div className="relative overflow-x-auto">
            <table className="w-full min-w-[800px] text-left text-sm">
              <thead className="text-xs text-ink-muted">
                <tr>
                  <th scope="col" className="px-5 py-2 font-medium">
                    Partition
                  </th>
                  <th scope="col" className="px-5 py-2 font-medium">
                    Owner (client id)
                  </th>
                  <th scope="col" className="px-5 py-2 text-right font-medium">
                    Committed
                  </th>
                  <th scope="col" className="px-5 py-2 text-right font-medium">
                    Log end
                  </th>
                  <th scope="col" className="px-5 py-2 font-medium">
                    Lag
                  </th>
                  <th scope="col" className="px-5 py-2 font-medium">
                    Stored / retention limit
                  </th>
                </tr>
              </thead>
              <tbody>
                {k.partitions.map((p) => (
                  <tr key={p.partition} className="border-t border-line/60">
                    <td className="tabular px-5 py-2 font-mono text-ink">{p.partition}</td>
                    <td className="px-5 py-2 font-mono text-xs text-ink-muted">
                      {p.owner ?? <span className="text-warn">unassigned</span>}
                    </td>
                    <td className="tabular px-5 py-2 text-right font-mono text-ink">
                      {formatCount(p.committedOffset) ?? '—'}
                    </td>
                    <td className="tabular px-5 py-2 text-right font-mono text-ink">
                      {formatCount(p.logEndOffset)}
                    </td>
                    <td className="px-5 py-2">
                      <div className="flex items-center gap-3">
                        <div
                          className="h-1.5 w-28 overflow-hidden rounded-full bg-panel-raised"
                          aria-hidden="true"
                        >
                          <div
                            className={`h-full rounded-full ${p.lag > 0 ? 'bg-warn' : 'bg-ok'}`}
                            style={{
                              width: `${p.lag === 0 ? 100 : Math.max(4, (p.lag / maxLag) * 100)}%`,
                            }}
                          />
                        </div>
                        <span className="tabular font-mono text-xs text-ink">
                          {formatCount(p.lag)}
                        </span>
                      </div>
                    </td>
                    <td className="px-5 py-2">
                      <RetentionFill size={p.sizeBytes} limit={k.retentionBytes} lag={p.lag} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="grid gap-4 lg:grid-cols-2">
        <section
          aria-labelledby="retry-title"
          className="rounded-xl border border-line bg-panel p-5"
        >
          <h2 id="retry-title" className="text-sm font-semibold text-ink">
            Retries and dead letters
          </h2>
          <p className="mt-0.5 text-xs text-ink-muted">
            Failed attempts per second across live workers (10 s). Bad records go straight to the
            dead-letter topic; transient failures get 3 attempts; infrastructure failures retry
            until recovery.
          </p>
          <dl className="mt-4 space-y-2 text-sm">
            {['BAD_RECORD', 'TRANSIENT', 'INFRASTRUCTURE'].map((c) => (
              <div key={c} className="flex justify-between">
                <dt className="text-ink-muted">{humanize(c)}</dt>
                <dd className="tabular font-mono text-ink">{formatRate(failed?.[c]) ?? '—'} /s</dd>
              </div>
            ))}
            <div className="flex justify-between border-t border-line/60 pt-2">
              <dt className="text-ink-muted">Dead-lettered, last minute</dt>
              <dd className="tabular font-mono text-ink">
                {formatCount(snapshot.throughput?.deadLetteredLastMinute) ?? '—'}
              </dd>
            </div>
            {snapshot.deadLetters &&
              Object.entries(snapshot.deadLetters).map(([status, count]) => (
                <div key={status} className="flex justify-between">
                  <dt className="text-ink-muted">Dead letters {humanize(status).toLowerCase()}</dt>
                  <dd className="tabular font-mono text-ink">{formatCount(count)}</dd>
                </div>
              ))}
          </dl>
        </section>

        <section
          aria-labelledby="workers-title"
          className="rounded-xl border border-line bg-panel p-5"
        >
          <h2 id="workers-title" className="text-sm font-semibold text-ink">
            Workers
          </h2>
          <p className="mt-0.5 text-xs text-ink-muted">
            Counters since each worker process started, from its latest heartbeat.
          </p>
          {!snapshot.workers || snapshot.workers.list.length === 0 ? (
            <p className="mt-4 text-sm text-ink-muted">No worker has sent a heartbeat recently.</p>
          ) : (
            <ul className="mt-4 space-y-3">
              {snapshot.workers.list.map((w) => (
                <li
                  key={w.workerId}
                  className="rounded-lg border border-line/80 bg-panel-raised p-3"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-mono text-sm text-ink">
                      {shortId(w.workerId)} <span className="text-ink-muted">on {w.host}</span>
                    </span>
                    <StatusPill
                      tone={!w.live ? 'bad' : w.inOutage ? 'warn' : 'ok'}
                      label={!w.live ? 'Not live' : w.inOutage ? 'In outage' : 'Live'}
                    />
                  </div>
                  <dl className="tabular mt-2 grid grid-cols-2 gap-x-4 gap-y-1 font-mono text-xs sm:grid-cols-4">
                    <WorkerStat label="processed" value={w.processed} />
                    <WorkerStat label="duplicates" value={w.duplicates} />
                    <WorkerStat label="failed attempts" value={w.failedAttempts} />
                    <WorkerStat label="dead-lettered" value={w.deadLettered} />
                  </dl>
                  <p className="mt-2 text-xs text-ink-muted">
                    Partitions {w.partitions.length ? w.partitions.join(', ') : 'none'} · seen{' '}
                    {formatAge(w.lastSeenAt)}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </>
  );
}

/**
 * How full a partition is relative to retention.bytes. Beyond the limit Kafka deletes the oldest records, read or
 * not; with unread records (lag) that would be an offset gap, so a lagging partition above 80 % is flagged.
 */
function RetentionFill({
  size,
  limit,
  lag,
}: {
  size: number | null;
  limit: number | null;
  lag: number;
}) {
  if (size === null || !limit) {
    return <span className="text-xs text-ink-muted">—</span>;
  }
  const fill = size / limit;
  const atRisk = lag > 0 && fill >= 0.8;
  return (
    <span className={`tabular font-mono text-xs ${atRisk ? 'text-bad' : 'text-ink'}`}>
      {formatBytes(size)} · {(Math.floor(fill * 1000) / 10).toFixed(1)} %
      {atRisk ? ' · at risk' : ''}
    </span>
  );
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024 ** 3) {
    return `${(bytes / 1024 ** 3).toFixed(2)} GiB`;
  }
  if (bytes >= 1024 ** 2) {
    return `${(bytes / 1024 ** 2).toFixed(1)} MiB`;
  }
  return `${Math.round(bytes / 1024)} KiB`;
}

function WorkerStat({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <dt className="text-ink-faint">{label}</dt>
      <dd className="text-ink">{formatCount(value)}</dd>
    </div>
  );
}

function IngestorGroup() {
  const query = useQuery({
    queryKey: ['consumer-groups'],
    queryFn: ({ signal }) => api.consumerGroups(signal),
    refetchInterval: 5_000,
  });
  if (query.isPending) {
    return <LoadingState label="Loading the dead-letter ingestion group…" />;
  }
  if (query.isError && !query.data) {
    return <ErrorState message={query.error.message} onRetry={() => void query.refetch()} />;
  }
  return <GroupCard group={query.data.deadLetterIngestion} title="Dead-letter ingestion group" />;
}

function GroupCard({ group, title }: { group: GroupState; title: string }) {
  const lag = group.partitions.reduce((sum, p) => sum + p.lag, 0);
  return (
    <section aria-label={title} className="rounded-xl border border-line bg-panel p-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-semibold text-ink">{title}</h2>
        <StatusPill
          tone={lag === 0 ? 'ok' : 'warn'}
          label={`${humanize(group.state)} · lag ${formatCount(lag)}`}
        />
      </div>
      <p className="mt-1 text-xs text-ink-muted">
        <span className="font-mono">{group.groupId}</span> on{' '}
        <span className="font-mono">{group.topic}</span> · {group.members.length} member(s) ·
        refreshed every 5 s
      </p>
    </section>
  );
}
