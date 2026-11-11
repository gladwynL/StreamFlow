import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { api, RANGES, type Range } from '../../api/endpoints';
import type { Series } from '../../api/types';
import { CategoryBars, ChartFrame, DataTable, SERIES, TimeLines } from '../../components/charts';
import {
  EmptyState,
  ErrorState,
  LoadingState,
  Metric,
  PageHeader,
  SyntheticBadge,
} from '../../components/ui';
import {
  formatAmount,
  formatCount,
  formatLatency,
  formatRate,
  humanize,
  shortId,
} from '../../lib/format';
import { fillMinutes } from '../../lib/series';

const minuteLabel = (iso: string) =>
  new Date(iso).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });

export function AnalyticsPage() {
  const [range, setRange] = useState<Range>('1h');
  const series = useQuery({
    queryKey: ['series', range],
    queryFn: ({ signal }) => api.series(range, signal),
    refetchInterval: 30_000,
  });
  const distribution = useQuery({
    queryKey: ['distribution', range],
    queryFn: ({ signal }) => api.distribution({ range }, signal),
    refetchInterval: 30_000,
  });

  return (
    <div className="space-y-6">
      <PageHeader
        title="Analytics"
        description={
          <>
            Historical processing from the per-minute aggregates and latency histogram in
            PostgreSQL. <SyntheticBadge className="align-middle" />
          </>
        }
        actions={
          <div
            role="radiogroup"
            aria-label="Time range"
            className="flex rounded-lg border border-line bg-panel p-0.5"
          >
            {RANGES.map((r) => (
              <button
                key={r}
                type="button"
                role="radio"
                aria-checked={range === r}
                onClick={() => setRange(r)}
                className={`rounded-md px-3 py-1 text-sm ${range === r ? 'bg-panel-raised font-medium text-ink' : 'text-ink-muted hover:text-ink'}`}
              >
                {r}
              </button>
            ))}
          </div>
        }
      />

      {series.isPending && <LoadingState />}
      {series.isError && !series.data && (
        <ErrorState message={series.error.message} onRetry={() => void series.refetch()} />
      )}
      {series.data && <SeriesCharts series={series.data} />}

      <ChartFrame
        title={`Events by type, last ${range}`}
        description="Processed events per event type, from the minute aggregates."
        table={
          <DataTable
            head={['Type', 'Events', 'Amount']}
            rows={(distribution.data?.types ?? []).map((t) => [
              humanize(t.eventType),
              formatCount(t.events),
              formatAmount(t.amountCents, 'USD'),
            ])}
          />
        }
      >
        {distribution.isError ? (
          <ErrorState message={distribution.error.message} />
        ) : (distribution.data?.types.length ?? 0) === 0 ? (
          <p className="flex h-[200px] items-center justify-center text-sm text-ink-muted">
            {distribution.isPending ? 'Loading…' : 'No events in this range.'}
          </p>
        ) : (
          <CategoryBars
            data={distribution.data?.types ?? []}
            categoryKey="eventType"
            valueKey="events"
            valueLabel="Events"
            categoryFormat={humanize}
            valueFormat={(v) => formatCount(v) ?? ''}
          />
        )}
      </ChartFrame>

      <RunAnalytics />
    </div>
  );
}

function SeriesCharts({ series }: { series: Series }) {
  const points = fillMinutes(series.points, series.from, series.to);
  const totalProcessed = points.reduce((sum, p) => sum + p.processed, 0);
  const totalDead = points.reduce((sum, p) => sum + p.deadLettered, 0);
  const peak = points.reduce((max, p) => Math.max(max, p.processedPerSecond), 0);
  const latency = series.latency;
  const histogram = latency.histogram
    .map((b, i) => ({
      bucket:
        b.upperBoundMillis === null
          ? `> ${latency.histogram[i - 1]?.upperBoundMillis ?? ''} ms`
          : `≤ ${b.upperBoundMillis} ms`,
      count: b.count,
    }))
    .filter((b) => b.count > 0);

  return (
    <>
      <section aria-label="Range totals" className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Metric
          label="Processed"
          value={formatCount(totalProcessed)}
          unit="events"
          hint={`Last ${series.range}`}
        />
        <Metric
          label="Peak minute"
          value={formatRate(peak)}
          unit="events/s"
          hint="Busiest minute, averaged over the minute"
        />
        <Metric
          label="Dead-lettered"
          value={formatCount(totalDead)}
          unit="records"
          hint={`Last ${series.range}`}
        />
        <Metric
          label="Latency p99"
          value={formatLatency(latency.p99Millis)}
          hint={`${formatCount(latency.samples)} samples · p50 ${formatLatency(latency.p50Millis) ?? '—'}, p95 ${formatLatency(latency.p95Millis) ?? '—'}`}
        />
      </section>

      {series.points.length === 0 ? (
        <EmptyState title={`No activity in the last ${series.range}`}>
          Start a simulation to generate synthetic traffic.
        </EmptyState>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          <ChartFrame
            title="Throughput"
            description="Processed events per second, averaged per minute."
            table={
              <DataTable
                head={['Minute', 'Processed', 'Per second', 'Dead-lettered']}
                rows={points.map((p) => [
                  minuteLabel(p.minute),
                  formatCount(p.processed),
                  formatRate(p.processedPerSecond),
                  formatCount(p.deadLettered),
                ])}
              />
            }
          >
            <TimeLines
              data={points}
              xKey="minute"
              series={[{ key: 'processedPerSecond', label: 'Processed/s' }]}
              xFormat={minuteLabel}
              yFormat={(v) => formatRate(v) ?? ''}
            />
          </ChartFrame>
          <ChartFrame
            title="Latency percentiles"
            description="Kafka append to database, per minute. Values are histogram bucket upper bounds."
            legend={[
              { label: 'p50', color: SERIES[0] ?? '' },
              { label: 'p95', color: SERIES[1] ?? '', dashed: true },
              { label: 'p99', color: SERIES[2] ?? '', dashed: true },
            ]}
            table={
              <DataTable
                head={['Minute', 'Samples', 'p50', 'p95', 'p99']}
                rows={points.map((p) => [
                  minuteLabel(p.minute),
                  formatCount(p.latencySamples),
                  formatLatency(p.p50Millis),
                  formatLatency(p.p95Millis),
                  formatLatency(p.p99Millis),
                ])}
              />
            }
          >
            <TimeLines
              data={points}
              xKey="minute"
              series={[
                { key: 'p50Millis', label: 'p50' },
                { key: 'p95Millis', label: 'p95' },
                { key: 'p99Millis', label: 'p99' },
              ]}
              dashedFrom={1}
              xFormat={minuteLabel}
              yFormat={(v) => (v >= 1000 ? `${v / 1000} s` : `${v} ms`)}
            />
          </ChartFrame>
          <ChartFrame
            title="Latency distribution"
            description={`All ${formatCount(latency.samples)} samples in the range, by histogram bucket.`}
            table={
              <DataTable
                head={['Bucket', 'Events']}
                rows={histogram.map((b) => [b.bucket, formatCount(b.count)])}
              />
            }
          >
            <CategoryBars
              data={histogram}
              categoryKey="bucket"
              valueKey="count"
              valueLabel="Events"
              categoryFormat={(v) => v}
              valueFormat={(v) => formatCount(v) ?? ''}
              height={Math.max(160, histogram.length * 26)}
            />
          </ChartFrame>
          <ChartFrame
            title="Dead letters per minute"
            description="Records stored as dead letters, by the minute they were dead-lettered."
            table={
              <DataTable
                head={['Minute', 'Dead-lettered']}
                rows={points.map((p) => [minuteLabel(p.minute), formatCount(p.deadLettered)])}
              />
            }
          >
            <TimeLines
              data={points}
              xKey="minute"
              series={[{ key: 'deadLettered', label: 'Dead-lettered' }]}
              xFormat={minuteLabel}
              yFormat={(v) => formatCount(v) ?? ''}
            />
          </ChartFrame>
        </div>
      )}
    </>
  );
}

function RunAnalytics() {
  const runs = useQuery({
    queryKey: ['runs', 'analytics'],
    queryFn: ({ signal }) => api.runs({ limit: 20 }, signal),
  });
  const [runId, setRunId] = useState('');
  const selected = runId || runs.data?.[0]?.runId || '';
  const detail = useQuery({
    queryKey: ['run', selected],
    queryFn: ({ signal }) => api.run(selected, signal),
    enabled: selected !== '',
  });

  return (
    <section aria-labelledby="run-analytics-title" className="space-y-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 id="run-analytics-title" className="text-lg font-semibold text-ink">
            Per-run analytics
          </h2>
          <p className="text-sm text-ink-muted">Event mix and outcomes of one simulation run.</p>
        </div>
        {runs.data && runs.data.length > 0 && (
          <label className="text-sm">
            <span className="text-ink-muted">Run</span>
            <select value={selected} onChange={(e) => setRunId(e.target.value)} className="field">
              {runs.data.map((r) => (
                <option key={r.runId} value={r.runId}>
                  {shortId(r.runId)} · {formatCount(r.requestedEvents)} events ·{' '}
                  {new Date(r.startedAt).toLocaleString()}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>
      {runs.isError && <ErrorState message={runs.error.message} />}
      {runs.data?.length === 0 && <EmptyState title="No simulation runs yet" />}
      {detail.data && (
        <div className="grid gap-4 lg:grid-cols-3">
          <div className="space-y-3">
            <Metric
              label="Processed"
              value={formatCount(detail.data.run.processed)}
              unit={`of ${formatCount(detail.data.run.requestedEvents)}`}
              hint="Distinct events in the ledger"
            />
            <Metric
              label="Duplicates suppressed"
              value={formatCount(detail.data.run.duplicatesSuppressed)}
              hint="Redeliveries absorbed by the ledger"
            />
            <Metric
              label="Dead letters"
              value={formatCount(detail.data.run.deadLetters)}
              hint="All statuses"
            />
          </div>
          <div className="lg:col-span-2">
            <ChartFrame
              title="Run event mix"
              description="Processed events of this run by type."
              table={
                <DataTable
                  head={['Type', 'Events']}
                  rows={detail.data.distribution.map((t) => [
                    humanize(t.eventType),
                    formatCount(t.events),
                  ])}
                />
              }
            >
              {detail.data.distribution.length === 0 ? (
                <p className="flex h-[200px] items-center justify-center text-sm text-ink-muted">
                  No events processed for this run.
                </p>
              ) : (
                <CategoryBars
                  data={detail.data.distribution}
                  categoryKey="eventType"
                  valueKey="events"
                  valueLabel="Events"
                  categoryFormat={humanize}
                  valueFormat={(v) => formatCount(v) ?? ''}
                />
              )}
            </ChartFrame>
          </div>
        </div>
      )}
    </section>
  );
}
