import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { DeadLetterSummary } from '../../api/types';
import { OPERATOR_HEADER } from '../../api/client';
import { jsonResponse, liveSnapshot } from '../../test/fixtures';
import { feedOf, renderApp } from '../../test/render';
import { DeadLettersPage, eligibility } from './DeadLettersPage';

const now = Date.now();
const recent = new Date(now - 3_600_000).toISOString();

function deadLetter(overrides: Partial<DeadLetterSummary>): DeadLetterSummary {
  return {
    id: 1,
    status: 'PENDING',
    failureCategory: 'BAD_RECORD',
    errorMessage: 'rejected',
    eventId: '0199a1b2-0000-7000-8000-000000000001',
    runId: '0199a1b2-aaaa-7000-8000-000000000000',
    runSeq: 3,
    sourceTopic: 'streamflow.events.v1',
    sourcePartition: 2,
    sourceOffset: 40,
    deadLetteredAt: recent,
    replayCount: 0,
    ...overrides,
  };
}

const rows = [
  deadLetter({ id: 11 }),
  deadLetter({ id: 10, eventId: null }),
  deadLetter({ id: 9, deadLetteredAt: new Date(now - 8 * 24 * 3_600_000).toISOString() }),
];

function mockFetch(handler: (url: string, init?: RequestInit) => Response) {
  const fetchMock = vi.fn((url: string, init?: RequestInit) => Promise.resolve(handler(url, init)));
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('eligibility', () => {
  it('mirrors the server rules for replay', () => {
    expect(eligibility(rows[0]!, now).replayable).toBe(true);
    expect(eligibility(rows[1]!, now).reason).toMatch(/No event id/);
    expect(eligibility(rows[2]!, now).reason).toMatch(/replay window/);
    expect(eligibility(deadLetter({ status: 'REPLAYED' }), now)).toEqual({
      replayable: false,
      reason: 'Replayed',
    });
  });
});

describe('DeadLettersPage', () => {
  it('lists dead letters with provenance and eligibility, and only replayable ones are selectable', async () => {
    mockFetch(() => jsonResponse(rows));
    renderApp(<DeadLettersPage />, { feed: feedOf(liveSnapshot()) });

    expect(await screen.findByText('#11')).toBeInTheDocument();
    expect(screen.getByLabelText('Select dead letter 11')).toBeEnabled();
    expect(screen.getByLabelText('Select dead letter 10')).toBeDisabled();
    expect(screen.getByLabelText('Select dead letter 9')).toBeDisabled();
    expect(screen.getAllByText(/streamflow\.events\.v1 \[2\] @ 40/)).toHaveLength(3);
    expect(screen.getByRole('list', { name: 'Dead letters by status' })).toHaveTextContent(
      'Pending 5',
    );
  });

  it('replays after confirmation and unlock, sending the key only in the header', async () => {
    const user = userEvent.setup();
    const fetchMock = mockFetch((_url, init) =>
      init?.method === 'POST'
        ? jsonResponse([{ id: 11, outcome: 'REPLAYED', detail: null }])
        : jsonResponse(rows),
    );
    renderApp(<DeadLettersPage />, { feed: feedOf(liveSnapshot()) });

    await user.click(await screen.findByLabelText('Select dead letter 11'));
    await user.click(screen.getByRole('button', { name: 'Replay selected (1)' }));
    const confirm = screen.getByRole('dialog', { name: 'Replay 1 dead letter(s)?' });
    await user.click(within(confirm).getByRole('button', { name: 'Replay' }));

    await user.type(screen.getByLabelText('Operator key'), 'op-key');
    await user.click(screen.getByRole('button', { name: 'Unlock' }));

    expect(await screen.findByText('Replay finished: 1 of 1 republished.')).toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === 'POST');
    expect(post?.[0]).toBe('/api/v1/dead-letters/replay');
    expect((post?.[1]?.headers as Record<string, string>)[OPERATOR_HEADER]).toBe('op-key');
    expect(post?.[1]?.body).toBe(JSON.stringify({ ids: [11] }));
    expect(fetchMock.mock.calls.every(([url]) => !String(url).includes('op-key'))).toBe(true);
  });

  it('requires a reason to discard and shows server errors in the dialog', async () => {
    const user = userEvent.setup();
    mockFetch((_url, init) =>
      init?.method === 'POST'
        ? jsonResponse({ detail: 'Operator rate limit exceeded' }, 429)
        : jsonResponse(rows),
    );
    renderApp(<DeadLettersPage />, { feed: feedOf(liveSnapshot()) });

    await user.click(await screen.findByRole('button', { name: 'Discard dead letter 11' }));
    const dialog = screen.getByRole('dialog', { name: 'Discard dead letter #11?' });
    const confirm = within(dialog).getByRole('button', { name: 'Discard permanently' });
    expect(confirm).toBeDisabled();
    await user.type(within(dialog).getByLabelText('Reason (required)'), 'duplicate test data');
    await user.click(confirm);
    await user.type(screen.getByLabelText('Operator key'), 'op-key');
    await user.click(screen.getByRole('button', { name: 'Unlock' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent(
      'Operator rate limit exceeded',
    );
  });

  it('sends nothing when the unlock is cancelled', async () => {
    const user = userEvent.setup();
    const fetchMock = mockFetch(() => jsonResponse(rows));
    renderApp(<DeadLettersPage />, { feed: feedOf(liveSnapshot()) });

    await user.click(await screen.findByLabelText('Select dead letter 11'));
    await user.click(screen.getByRole('button', { name: 'Replay selected (1)' }));
    await user.click(
      within(screen.getByRole('dialog', { name: 'Replay 1 dead letter(s)?' })).getByRole('button', {
        name: 'Replay',
      }),
    );
    const unlock = screen.getByRole('dialog', { name: 'Unlock operator actions' });
    await user.click(within(unlock).getByRole('button', { name: 'Cancel' }));

    await waitFor(() => expect(screen.getByText(/Operator key not entered/)).toBeInTheDocument());
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'POST')).toBe(false);
  });
});
