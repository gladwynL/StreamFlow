import { useQuery } from '@tanstack/react-query';
import { KeyValue, Panel } from '../../components/Panel';
import { StatusPill } from '../../components/StatusPill';
import {
  fetchSystemStatus,
  systemStatusQueryKey,
  type SystemStatus,
  type TopicStatus,
  type WorkerStatus,
} from '../../api/systemStatus';
import { formatAge, formatTime, shortId } from '../../lib/format';

export const REFRESH_INTERVAL_MS = 5000;

export function SystemStatusPage() {
  const query = useQuery({
    queryKey: systemStatusQueryKey,
    queryFn: ({ signal }) => fetchSystemStatus(signal),
    refetchInterval: REFRESH_INTERVAL_MS,
  });

  return (
    <div className="space-y-6">
      <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-ink">System status</h1>
          <p className="mt-1 text-sm text-ink-muted">
            Live checks of Kafka, PostgreSQL and the workers, made by the API on every refresh.
          </p>
        </div>
        {query.data && (
          <p className="text-xs text-ink-muted" aria-live="polite">
            Checked {formatTime(query.data.checkedAt)} · refreshes every{' '}
            {REFRESH_INTERVAL_MS / 1000}s
          </p>
        )}
      </header>

      {query.isPending && <LoadingState />}
      {query.isError && !query.data && (
        <ErrorState message={query.error.message} onRetry={() => void query.refetch()} />
      )}
      {query.data && (
        <>
          {query.isError && (
            <p
              role="alert"
              className="rounded-lg border border-warn/40 bg-warn/10 px-4 py-3 text-sm text-warn"
            >
              The latest refresh failed ({query.error.message}). Showing the result from{' '}
              {formatTime(query.data.checkedAt)}.
            </p>
          )}
          <StatusDetails status={query.data} />
        </>
      )}
    </div>
  );
}

function StatusDetails({ status }: { status: SystemStatus }) {
  const liveWorkers = status.workers.filter((worker) => worker.live).length;
  return (
    <>
      <OverallBanner status={status} />

      <div className="grid gap-4 md:grid-cols-3">
        <Panel
          title="Kafka"
          aside={
            <StatusPill
              tone={status.kafka.reachable ? 'ok' : 'bad'}
              label={status.kafka.reachable ? 'Reachable' : 'Unreachable'}
            />
          }
        >
          <dl>
            <KeyValue label="Cluster ID" value={status.kafka.clusterId ?? '—'} />
            <KeyValue label="Brokers" value={status.kafka.brokerCount ?? '—'} />
            <KeyValue label="Topics present" value={countPresent(status.kafka.topics)} />
          </dl>
          {status.kafka.error && (
            <p className="mt-3 text-xs break-words text-bad">{status.kafka.error}</p>
          )}
        </Panel>

        <Panel
          title="PostgreSQL"
          aside={
            <StatusPill
              tone={status.database.reachable ? 'ok' : 'bad'}
              label={status.database.reachable ? 'Reachable' : 'Unreachable'}
            />
          }
        >
          <dl>
            <KeyValue label="Server version" value={status.database.serverVersion ?? '—'} />
            <KeyValue
              label="Schema version"
              value={status.database.schemaVersion ? `V${status.database.schemaVersion}` : '—'}
            />
          </dl>
          {status.database.error && (
            <p className="mt-3 text-xs break-words text-bad">{status.database.error}</p>
          )}
        </Panel>

        <Panel
          title="Workers"
          aside={
            <StatusPill
              tone={liveWorkers > 0 ? 'ok' : 'warn'}
              label={liveWorkers > 0 ? `${liveWorkers} live` : 'None live'}
            />
          }
        >
          <dl>
            <KeyValue label="Live" value={liveWorkers} />
            <KeyValue label="Seen in the last 10 min" value={status.workers.length} />
          </dl>
        </Panel>
      </div>

      <TopicsTable topics={status.kafka.topics} />
      <WorkersTable workers={status.workers} checkedAt={status.checkedAt} />
    </>
  );
}

function OverallBanner({ status }: { status: SystemStatus }) {
  const ok = status.status === 'OK';
  return (
    <section
      aria-labelledby="overall-heading"
      className={`rounded-xl border p-5 ${ok ? 'border-ok/30 bg-ok/5' : 'border-warn/40 bg-warn/5'}`}
    >
      <div className="flex items-center gap-3">
        <StatusPill tone={ok ? 'ok' : 'warn'} label={ok ? 'Operational' : 'Degraded'} />
        <h2 id="overall-heading" className="text-base font-medium text-ink">
          {ok ? 'All checks passed' : `${status.issues.length} issue(s) need attention`}
        </h2>
      </div>
      {!ok && (
        <ul className="mt-3 list-disc space-y-1 pl-6 text-sm text-ink">
          {status.issues.map((issue) => (
            <li key={issue}>{issue}</li>
          ))}
        </ul>
      )}
    </section>
  );
}

function TopicsTable({ topics }: { topics: TopicStatus[] }) {
  return (
    <Panel title="Topics">
      {topics.length === 0 ? (
        <p className="text-sm text-ink-muted">
          Topic state is unavailable while Kafka is unreachable.
        </p>
      ) : (
        <div className="relative overflow-x-auto">
          <table className="w-full min-w-[36rem] text-left text-sm">
            <thead className="text-xs text-ink-muted uppercase">
              <tr>
                <th scope="col" className="pb-2 font-medium">
                  Topic
                </th>
                <th scope="col" className="pb-2 font-medium">
                  State
                </th>
                <th scope="col" className="pb-2 text-right font-medium">
                  Partitions
                </th>
                <th scope="col" className="pb-2 text-right font-medium">
                  Replication
                </th>
                <th scope="col" className="pb-2 pl-6 font-medium">
                  Config drift
                </th>
              </tr>
            </thead>
            <tbody>
              {topics.map((topic) => (
                <tr key={topic.name} className="border-t border-line/60">
                  <td className="py-2.5 font-mono text-ink">{topic.name}</td>
                  <td className="py-2.5">
                    <StatusPill tone={topicTone(topic)} label={topicLabel(topic)} />
                  </td>
                  <td className="py-2.5 text-right font-mono">
                    {topic.partitions ?? '—'}
                    <span className="text-ink-muted"> / {topic.expectedPartitions}</span>
                  </td>
                  <td className="py-2.5 text-right font-mono">{topic.replicationFactor ?? '—'}</td>
                  <td className="py-2.5 pl-6 text-ink-muted">
                    {topic.configDrift.length === 0
                      ? 'None'
                      : topic.configDrift
                          .map((d) => `${d.key}: ${d.actual ?? 'unset'} → ${d.expected}`)
                          .join(', ')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

function WorkersTable({ workers, checkedAt }: { workers: WorkerStatus[]; checkedAt: string }) {
  return (
    <Panel title="Worker heartbeats">
      {workers.length === 0 ? (
        <p className="text-sm text-ink-muted">No worker has reported in the last 10 minutes.</p>
      ) : (
        <div className="relative overflow-x-auto">
          <table className="w-full min-w-[36rem] text-left text-sm">
            <thead className="text-xs text-ink-muted uppercase">
              <tr>
                <th scope="col" className="pb-2 font-medium">
                  Worker
                </th>
                <th scope="col" className="pb-2 font-medium">
                  Host
                </th>
                <th scope="col" className="pb-2 font-medium">
                  State
                </th>
                <th scope="col" className="pb-2 text-right font-medium">
                  Last heartbeat
                </th>
              </tr>
            </thead>
            <tbody>
              {workers.map((worker) => (
                <tr key={worker.workerId} className="border-t border-line/60">
                  <td className="py-2.5 font-mono text-ink" title={worker.workerId}>
                    {shortId(worker.workerId)}
                  </td>
                  <td className="py-2.5 font-mono text-ink-muted">{worker.host}</td>
                  <td className="py-2.5">
                    <StatusPill
                      tone={worker.live ? 'ok' : 'neutral'}
                      label={worker.live ? 'Live' : 'Stale'}
                    />
                  </td>
                  <td className="py-2.5 text-right text-ink-muted">
                    {formatAge(worker.lastSeenAt, new Date(checkedAt))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

function LoadingState() {
  return (
    <div aria-busy="true" aria-label="Loading system status" className="space-y-4">
      <div className="h-16 animate-pulse rounded-xl bg-panel" />
      <div className="grid gap-4 md:grid-cols-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="h-36 animate-pulse rounded-xl bg-panel" />
        ))}
      </div>
    </div>
  );
}

function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div role="alert" className="rounded-xl border border-bad/40 bg-bad/5 p-6">
      <h2 className="text-base font-medium text-ink">Could not reach the StreamFlow API</h2>
      <p className="mt-1 text-sm text-ink-muted">{message}</p>
      <button
        type="button"
        onClick={onRetry}
        className="mt-4 rounded-lg border border-line bg-panel-raised px-3 py-1.5 text-sm font-medium text-ink hover:border-accent"
      >
        Try again
      </button>
    </div>
  );
}

function countPresent(topics: TopicStatus[]): string {
  if (topics.length === 0) {
    return '—';
  }
  return `${topics.filter((t) => t.present).length} of ${topics.length}`;
}

function topicTone(topic: TopicStatus) {
  if (!topic.present) {
    return 'bad' as const;
  }
  const healthy = topic.partitions === topic.expectedPartitions && topic.configDrift.length === 0;
  return healthy ? ('ok' as const) : ('warn' as const);
}

function topicLabel(topic: TopicStatus): string {
  if (!topic.present) {
    return 'Missing';
  }
  if (topic.partitions !== topic.expectedPartitions) {
    return 'Wrong partitions';
  }
  return topic.configDrift.length === 0 ? 'Healthy' : 'Config drift';
}
