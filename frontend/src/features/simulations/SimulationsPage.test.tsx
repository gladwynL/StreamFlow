import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { OPERATOR_HEADER } from '../../api/client';
import { jsonResponse, liveSnapshot } from '../../test/fixtures';
import { feedOf, renderApp } from '../../test/render';
import { SimulationsPage } from './SimulationsPage';
import { DEFAULT_FORM, toRequest } from './runRequest';

describe('toRequest', () => {
  it('builds a bounded request and omits defaults', () => {
    expect(toRequest(DEFAULT_FORM)).toEqual({
      request: { ratePerSecond: 1000, events: 20000, users: 10000 },
    });
    expect(
      toRequest({
        ...DEFAULT_FORM,
        mode: 'duration',
        durationSeconds: '60',
        seed: '7',
        types: ['USER_LOGIN'],
      }),
    ).toEqual({
      request: {
        ratePerSecond: 1000,
        durationSeconds: 60,
        users: 10000,
        seed: 7,
        eventTypes: ['USER_LOGIN'],
      },
    });
  });

  it('rejects runs beyond the server limits', () => {
    expect(toRequest({ ...DEFAULT_FORM, events: '500001' }).error).toMatch(/Events/);
    expect(toRequest({ ...DEFAULT_FORM, rate: '5001' }).error).toMatch(/Rate/);
    expect(toRequest({ ...DEFAULT_FORM, events: '6001', rate: '10' }).error).toMatch(
      /longer than 600 s/,
    );
    expect(
      toRequest({ ...DEFAULT_FORM, mode: 'duration', durationSeconds: '200', rate: '5000' }).error,
    ).toMatch(/limit is 500,000/);
    expect(toRequest({ ...DEFAULT_FORM, types: [] }).error).toMatch(/at least one event type/);
    expect(toRequest({ ...DEFAULT_FORM, seed: '-1' }).error).toMatch(/Seed/);
  });
});

describe('SimulationsPage', () => {
  it('starts a run with the operator key in the header and reports the server response', async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn((_url: string, init?: RequestInit) =>
      Promise.resolve(
        init?.method === 'POST'
          ? jsonResponse(
              {
                runId: '0199a1b2-aaaa-7000-8000-000000000000',
                seed: 99,
                events: 20000,
                ratePerSecond: 1000,
                users: 10000,
                eventTypes: [],
              },
              202,
            )
          : jsonResponse([]),
      ),
    );
    vi.stubGlobal('fetch', fetchMock);
    renderApp(<SimulationsPage />, { feed: feedOf(liveSnapshot()) });

    await user.click(screen.getByRole('button', { name: 'Start synthetic run' }));
    await user.type(screen.getByLabelText('Operator key'), 'op-key');
    await user.click(screen.getByRole('button', { name: 'Unlock' }));

    expect(
      await screen.findByText(/Run 0199a1b2 started: 20,000 events at 1,000\/s, seed 99/),
    ).toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === 'POST');
    expect(post?.[0]).toBe('/api/v1/simulations');
    expect((post?.[1]?.headers as Record<string, string>)[OPERATOR_HEADER]).toBe('op-key');
  });

  it('shows client-side validation before contacting the server', async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(() => Promise.resolve(jsonResponse([])));
    vi.stubGlobal('fetch', fetchMock);
    renderApp(<SimulationsPage />, { feed: feedOf(liveSnapshot()) });

    const rate = screen.getByLabelText(/Rate/);
    await user.clear(rate);
    await user.type(rate, '9000');
    await user.click(screen.getByRole('button', { name: 'Start synthetic run' }));
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Rate must be a whole number from 1 to 5,000',
    );
    expect(screen.queryByLabelText('Operator key')).not.toBeInTheDocument();
  });

  it('prevents overlapping runs and shows live progress of the active run', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse([]))),
    );
    renderApp(<SimulationsPage />, {
      feed: feedOf(
        liveSnapshot({
          activeRun: {
            runId: '0199a1b2-aaaa-7000-8000-000000000000',
            phase: 'PUBLISHING',
            requestedEvents: 10_000,
            ratePerSecond: 1_000,
            attempted: 4_000,
            acked: 3_990,
            failed: 0,
            processed: 3_500,
            startedAt: '2026-11-11T09:59:55Z',
          },
        }),
      ),
    });

    expect(screen.getByRole('button', { name: 'A run is in progress' })).toBeDisabled();
    expect(
      screen.getByRole('meter', { name: 'Published (acknowledged by Kafka)' }),
    ).toHaveAttribute('aria-valuenow', '3990');
    expect(screen.getByRole('meter', { name: 'Processed by the workers' })).toHaveAttribute(
      'aria-valuenow',
      '3500',
    );
    expect(screen.getByRole('button', { name: 'Stop run' })).toBeInTheDocument();
    expect(screen.getAllByText('Synthetic').length).toBeGreaterThan(0);
  });
});
