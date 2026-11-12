import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router';
import { api } from '../../api/endpoints';
import type { LiveSnapshot } from '../../api/types';
import { ChartFrame, DataTable, TimeLines } from '../../components/charts';
import { SERIES } from '../../components/chartColors';
import { ErrorState, Metric, Notice, PageHeader, SyntheticBadge } from '../../components/ui';
import { useLive } from '../../live/liveContext';
import { LiveGate } from '../../live/LiveGate';
import {
  formatCount,
  formatLatency,
  formatPercent,
  formatRate,
  formatTime,
  humanize,
} from '../../lib/format';
import { fillMinutes } from '../../lib/series';
import { appliedShare } from './appliedShare';

export function OverviewPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Overview"
        description={
          <>
            Processing health at a glance, updated every second from the live feed. Traffic is
            produced by the simulator <SyntheticBadge className="ml-1 align-middle" />
          </>
        }
      />
      <LiveGate>{(snapshot) => <OverviewBody snapshot={snapshot} />}</LiveGate>
    </div>
  );
}

function OverviewBody({ snapshot }: { snapshot: LiveSnapshot }) {
  const { trend } = useLive();
  const t = snapshot.throughput;
  const failed = t?.failedAttemptsPerSecond;
  const failedTotal = failed ? Object.values(failed).reduce((a, b) => a + b, 0) : null;
  const share = appliedShare(snapshot);
  const pending = snapshot.deadLetters?.PENDING ?? null;

  return (
    <>
      {snapshot.databaseError && (
        <Notice tone="bad">PostgreSQL could not be queried: {snapshot.databaseError}</Notice>
      )}
      {snapshot.kafka.error && (
        <Notice tone="bad">Kafka could not be queried: {snapshot.kafka.error}</Notice>
      )}
      {snapshot.workers?.anyInOutage && (
        <Notice tone="warn">
          A worker reports an infrastructure outage: processing is paused and will resume on
          recovery.
        </Notice>
      )}

      <section aria-label="Throughput" className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Metric
          label="Processed"
          value={formatRate(t?.processedPerSecond)}
          unit="events/s"
          hint="First deliveries applied, 10 s average"
        />
        <Metric
          label="Published"
          value={formatRate(t?.publishedPerSecond)}
          unit="records/s"
          hint="Kafka log-end offset growth, event topic"
        />
        <Metric
          label="Processed last minute"
          value={formatCount(t?.processedLastMinute)}
          unit="events"
          hint="Rows in the event store, 60 s"
        />
        <Metric
          label="Consumer lag"
          value={formatCount(snapshot.kafka.totalLag)}
          unit="records"
          hint={`Group ${snapshot.kafka.groupState ?? 'state unknown'}, all partitions`}
          tone={
            snapshot.kafka.totalLag !== null && snapshot.kafka.totalLag > 10_000
              ? 'warn'
              : undefined
          }
        />
      </section>

      <section
        aria-label="Outcomes and latency"
        className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4"
      >
        <Metric
          label="Applied share"
          value={formatPercent(share)}
          unit="%"
          hint="Applied ÷ (applied + dead-lettered), 60 s"
          tone={share !== null && share < 0.99 ? 'warn' : undefined}
        />
        <Metric
          label="Failed attempts"
          value={formatRate(failedTotal)}
          unit="/s"
          hint="Across live workers, 10 s; includes retried attempts"
          tone={failedTotal ? 'warn' : undefined}
        />
        <Metric
          label="Latency p95"
          value={formatLatency(snapshot.latency?.p95Millis)}
          hint={`Kafka append → DB, 5 min, ${formatCount(snapshot.latency?.samples) ?? '—'} samples`}
        />
        <Metric
          label="Workers live"
          value={snapshot.workers ? `${snapshot.workers.live}` : null}
          unit={snapshot.workers ? `of ${snapshot.workers.seen} seen` : undefined}
          hint="Heartbeat within the last 6 s"
          tone={snapshot.workers && snapshot.workers.live === 0 ? 'bad' : undefined}
        />
      </section>

      <div className="grid gap-4 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <ChartFrame
            title="Throughput since this page opened"
            description="Published and processed per second, one point per live snapshot (last 5 minutes kept in this tab)."
            legend={[
              { label: 'Processed (events/s)', color: SERIES[0] ?? '' },
              { label: 'Published (records/s)', color: SERIES[1] ?? '', dashed: true },
            ]}
            table={
              <DataTable
                head={['Time', 'Processed/s', 'Published/s', 'Lag']}
                rows={trend
                  .slice(-60)
                  .map((p) => [
                    formatTime(p.at),
                    formatRate(p.processed),
                    formatRate(p.published),
                    formatCount(p.lag),
                  ])}
              />
            }
          >
            {trend.length < 2 ? (
              <p className="flex h-[220px] items-center justify-center text-sm text-ink-muted">
                Collecting live snapshots…
              </p>
            ) : (
              <TimeLines
                data={trend}
                xKey="at"
                series={[
                  { key: 'processed', label: 'Processed' },
                  { key: 'published', label: 'Published' },
                ]}
                dashedFrom={1}
                xFormat={(v) => formatTime(v)}
                yFormat={(v) => formatRate(v) ?? ''}
              />
            )}
          </ChartFrame>
        </div>
        <div className="space-y-3">
          <LatencyCard snapshot={snapshot} />
          <div className="rounded-xl border border-line bg-panel p-5">
            <p className="text-sm font-semibold text-ink">Failures</p>
            <dl className="mt-3 space-y-2 text-sm">
              {['BAD_RECORD', 'TRANSIENT', 'INFRASTRUCTURE'].map((category) => (
                <div key={category} className="flex justify-between">
                  <dt className="text-ink-muted">{humanize(category)} attempts/s</dt>
                  <dd className="tabular font-mono text-ink">
                    {formatRate(failed?.[category]) ?? '—'}
                  </dd>
                </div>
              ))}
              <div className="flex justify-between">
                <dt className="text-ink-muted">Duplicates suppressed/s</dt>
                <dd className="tabular font-mono text-ink">
                  {formatRate(t?.duplicatesPerSecond) ?? '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-ink-muted">Dead-lettered, last minute</dt>
                <dd className="tabular font-mono text-ink">
                  {formatCount(t?.deadLetteredLastMinute) ?? '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-ink-muted">Dead letters pending</dt>
                <dd className="tabular font-mono text-ink">
                  {pending === null ? (
                    '—'
                  ) : (
                    <Link
                      className="text-accent underline-offset-2 hover:underline"
                      to="/dead-letters"
                    >
                      {formatCount(pending)}
                    </Link>
                  )}
                </dd>
              </div>
            </dl>
          </div>
        </div>
      </div>

      <HistoricalStrip />
    </>
  );
}

function LatencyCard({ snapshot }: { snapshot: LiveSnapshot }) {
  const l = snapshot.latency;
  return (
    <div className="rounded-xl border border-line bg-panel p-5">
      <p className="text-sm font-semibold text-ink">Processing latency, 5 min</p>
      <p className="mt-0.5 text-xs text-ink-muted">
        Bucket upper bounds from the latency histogram
      </p>
      <dl className="mt-3 grid grid-cols-3 gap-2 text-center">
        {(
          [
            ['p50', l?.p50Millis],
            ['p95', l?.p95Millis],
            ['p99', l?.p99Millis],
          ] as const
        ).map(([label, value]) => (
          <div key={label} className="rounded-lg bg-panel-raised px-2 py-2">
            <dt className="text-xs text-ink-muted">{label}</dt>
            <dd className="tabular mt-0.5 font-mono text-sm text-ink">
              {formatLatency(value) ?? '—'}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function HistoricalStrip() {
  const query = useQuery({
    queryKey: ['series', '1h'],
    queryFn: ({ signal }) => api.series('1h', signal),
    refetchInterval: 30_000,
  });
  if (query.isError && !query.data) {
    return <ErrorState message={query.error.message} onRetry={() => void query.refetch()} />;
  }
  const points = query.data ? fillMinutes(query.data.points, query.data.from, query.data.to) : [];
  return (
    <ChartFrame
      title="Processed per minute, last hour"
      description="From the minute aggregates in PostgreSQL. Minutes without activity are not plotted."
      table={
        <DataTable
          head={['Minute', 'Processed', 'Dead-lettered']}
          rows={points.map((p) => [
            formatTime(p.minute),
            formatCount(p.processed),
            formatCount(p.deadLettered),
          ])}
        />
      }
    >
      {(query.data?.points.length ?? 0) === 0 ? (
        <p className="flex h-[180px] items-center justify-center text-sm text-ink-muted">
          {query.isPending
            ? 'Loading…'
            : 'No events processed in the last hour. Start a simulation to see activity.'}
        </p>
      ) : (
        <TimeLines
          data={points}
          xKey="minute"
          series={[{ key: 'processed', label: 'Processed' }]}
          xFormat={(v) =>
            new Date(v).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })
          }
          yFormat={(v) => formatCount(v) ?? ''}
          height={180}
        />
      )}
    </ChartFrame>
  );
}
