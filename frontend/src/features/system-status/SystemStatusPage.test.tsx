import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { healthyStatus, jsonResponse } from '../../test/fixtures';
import { SystemStatusPage } from './SystemStatusPage';

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <SystemStatusPage />
    </QueryClientProvider>,
  );
}

describe('SystemStatusPage', () => {
  it('requests the status endpoint and shows a healthy system', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(healthyStatus()));
    vi.stubGlobal('fetch', fetchMock);

    renderPage();

    expect(await screen.findByText('All checks passed')).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/system/status', expect.anything());

    const kafka = screen.getByRole('region', { name: 'Kafka' });
    expect(within(kafka).getByText('Reachable')).toBeInTheDocument();
    expect(within(kafka).getByText('c3RyZWFtZmxvdy1sb2NhbA')).toBeInTheDocument();

    const topics = screen.getByRole('region', { name: 'Topics' });
    expect(within(topics).getByText('streamflow.events.v1')).toBeInTheDocument();
    expect(within(topics).getAllByText('Healthy')).toHaveLength(2);

    const workers = screen.getByRole('region', { name: 'Worker heartbeats' });
    expect(within(workers).getByText('5f0c2a9e')).toBeInTheDocument();
    expect(within(workers).getByText('3s ago')).toBeInTheDocument();
  });

  it('lists every issue when the system is degraded', async () => {
    const degraded = healthyStatus({
      status: 'DEGRADED',
      issues: [
        'Topic streamflow.events.v1.dlt is missing',
        'No worker has sent a heartbeat within 15s',
      ],
      workers: [],
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(degraded)));

    renderPage();

    expect(await screen.findByText('2 issue(s) need attention')).toBeInTheDocument();
    expect(screen.getByText('Topic streamflow.events.v1.dlt is missing')).toBeInTheDocument();
    expect(screen.getByText('No worker has reported in the last 10 minutes.')).toBeInTheDocument();
  });

  it('shows Kafka errors without hiding the database state', async () => {
    const kafkaDown = healthyStatus({
      status: 'DEGRADED',
      issues: ['Kafka is unreachable: Kafka did not answer describe cluster within PT5S'],
      kafka: {
        reachable: false,
        clusterId: null,
        brokerCount: null,
        topics: [],
        error: 'Kafka did not answer describe cluster within PT5S',
      },
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(kafkaDown)));

    renderPage();

    const kafka = await screen.findByRole('region', { name: 'Kafka' });
    expect(within(kafka).getByText('Unreachable')).toBeInTheDocument();
    expect(
      screen.getByText('Topic state is unavailable while Kafka is unreachable.'),
    ).toBeInTheDocument();
    const database = screen.getByRole('region', { name: 'PostgreSQL' });
    expect(within(database).getByText('18.6')).toBeInTheDocument();
  });

  it('shows an error with a working retry when the API cannot be reached', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ title: 'Service Unavailable' }, 503))
      .mockResolvedValueOnce(jsonResponse(healthyStatus()));
    vi.stubGlobal('fetch', fetchMock);

    renderPage();

    const alert = await screen.findByRole('alert');
    expect(within(alert).getByText('Could not reach the StreamFlow API')).toBeInTheDocument();
    expect(within(alert).getByText(/HTTP 503/)).toBeInTheDocument();

    await userEvent.click(within(alert).getByRole('button', { name: 'Try again' }));

    expect(await screen.findByText('All checks passed')).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});
