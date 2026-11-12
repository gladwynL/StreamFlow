import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { liveSnapshot } from '../../test/fixtures';
import { appTree, feedOf, testQueryClient } from '../../test/render';
import { LiveStreamPage } from './LiveStreamPage';
import { mergeStream, toItems, type StreamItem } from './stream';

function item(key: string, at: string): StreamItem {
  return { key, at, status: 'PROCESSED', eventType: 'USER_LOGIN' };
}

describe('mergeStream', () => {
  it('keeps the newest rows first, without duplicates, within the limit', () => {
    const history = [item('a', '2026-11-11T10:00:02Z'), item('b', '2026-11-11T10:00:01Z')];
    const merged = mergeStream(
      history,
      [item('c', '2026-11-11T10:00:03Z'), item('a', '2026-11-11T10:00:02Z')],
      2,
    );
    expect(merged.map((i) => i.key)).toEqual(['c', 'a']);
    expect(mergeStream(history, [item('a', '2026-11-11T10:00:02Z')])).toBe(history);
  });

  it('turns a snapshot into processed, replayed and dead-lettered rows', () => {
    const snapshot = liveSnapshot();
    snapshot.recentEvents![0]!.replayed = true;
    expect(toItems(snapshot).map((i) => i.status)).toEqual(['REPLAYED', 'DEAD_LETTERED']);
  });
});

describe('LiveStreamPage', () => {
  it('filters by status, and holds new rows while paused until resumed', async () => {
    const user = userEvent.setup();
    const client = testQueryClient();
    const view = render(appTree(<LiveStreamPage />, feedOf(liveSnapshot()), client));

    expect(screen.getByRole('cell', { name: 'Purchase completed' })).toBeInTheDocument();
    expect(screen.getByRole('cell', { name: 'Bad record' })).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Status'), 'DEAD_LETTERED');
    expect(screen.queryByRole('cell', { name: 'Purchase completed' })).not.toBeInTheDocument();
    expect(screen.getByRole('cell', { name: 'Bad record' })).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText('Status'), '');

    await user.click(screen.getByRole('button', { name: 'Pause' }));
    expect(screen.getByRole('button', { name: 'Resume' })).toHaveAttribute('aria-pressed', 'true');

    const next = liveSnapshot({ sequence: 43 });
    next.recentEvents = [
      {
        ...next.recentEvents![0]!,
        eventId: 'new-event',
        eventType: 'USER_LOGIN',
        ingestedAt: '2026-11-11T10:00:01Z',
      },
    ];
    view.rerender(appTree(<LiveStreamPage />, feedOf(next), client));
    expect(screen.queryByRole('cell', { name: 'User login' })).not.toBeInTheDocument();
    expect(screen.getByText(/Paused · 1 new rows waiting/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Resume' }));
    expect(screen.getByRole('cell', { name: 'User login' })).toBeInTheDocument();
    expect(screen.getByRole('cell', { name: 'Purchase completed' })).toBeInTheDocument();
  });
});
