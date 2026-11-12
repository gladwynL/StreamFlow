import { screen } from '@testing-library/react';
import { jsonResponse, liveSnapshot } from '../../test/fixtures';
import { feedOf, renderApp } from '../../test/render';
import { appliedShare } from './appliedShare';
import { OverviewPage } from './OverviewPage';

const series = {
  range: '1h',
  from: '',
  to: '',
  points: [],
  latency: { samples: 0, p50Millis: null, p95Millis: null, p99Millis: null, histogram: [] },
};

function metric(label: string): HTMLElement {
  return screen.getByText(label, { selector: 'p' }).parentElement!;
}

describe('OverviewPage', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse(series))),
    );
  });

  it('shows exactly the measured values of the snapshot, each with its source', () => {
    renderApp(<OverviewPage />, { feed: feedOf(liveSnapshot()) });

    expect(metric('Processed')).toHaveTextContent('987events/s');
    expect(metric('Processed')).toHaveTextContent('First deliveries applied, 10 s average');
    expect(metric('Published')).toHaveTextContent('1,013records/s');
    expect(metric('Published')).toHaveTextContent('Kafka log-end offset growth');
    expect(metric('Consumer lag')).toHaveTextContent('321records');
    expect(metric('Latency p95')).toHaveTextContent('≤ 70 ms');
    expect(metric('Latency p95')).toHaveTextContent('290,000 samples');
    expect(metric('Workers live')).toHaveTextContent('1of 2 seen');
    expect(metric('Failed attempts')).toHaveTextContent('1.7/s');
    expect(metric('Applied share')).toHaveTextContent('99.9%');
  });

  it('renders unmeasured values as a dash, never as zero', () => {
    renderApp(<OverviewPage />, {
      feed: feedOf(
        liveSnapshot({
          throughput: null,
          latency: null,
          workers: null,
          deadLetters: null,
          databaseError: 'connection refused',
          kafka: {
            groupState: null,
            members: null,
            totalLag: null,
            logEndOffset: null,
            retentionBytes: null,
            partitions: null,
            error: 'timeout',
          },
        }),
      ),
    });
    for (const label of [
      'Processed',
      'Published',
      'Consumer lag',
      'Latency p95',
      'Workers live',
      'Applied share',
    ]) {
      expect(metric(label)).toHaveTextContent('—');
    }
    expect(
      screen.getByText(/PostgreSQL could not be queried: connection refused/),
    ).toBeInTheDocument();
    expect(screen.getByText(/Kafka could not be queried: timeout/)).toBeInTheDocument();
  });

  it('computes the applied share only from final outcomes', () => {
    expect(appliedShare(liveSnapshot())).toBeCloseTo(59_100 / 59_112);
    const idle = liveSnapshot();
    idle.throughput!.processedLastMinute = 0;
    idle.throughput!.deadLetteredLastMinute = 0;
    expect(appliedShare(idle)).toBeNull();
  });

  it('keeps the last data visible but marked when the feed disconnects', () => {
    renderApp(<OverviewPage />, { feed: feedOf(liveSnapshot(), { status: 'reconnecting' }) });
    expect(screen.getByText(/Disconnected from the live feed/)).toBeInTheDocument();
    expect(metric('Consumer lag')).toHaveTextContent('321');
  });

  it('shows a connecting state before the first snapshot', () => {
    renderApp(<OverviewPage />, { feed: feedOf(null) });
    expect(screen.getByRole('status')).toHaveTextContent('Connecting to the live feed');
  });
});
