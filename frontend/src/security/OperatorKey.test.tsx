import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { OperatorKeyProvider } from './OperatorKey';
import { useOperator } from './operatorContext';

function Probe() {
  const { requireKey, unlocked, lock } = useOperator();
  const [got, setGot] = useState<string>('none');
  return (
    <div>
      <p>state: {unlocked ? 'unlocked' : 'locked'}</p>
      <p>got: {got}</p>
      <button type="button" onClick={() => void requireKey().then((k) => setGot(k ?? 'cancelled'))}>
        act
      </button>
      <button type="button" onClick={lock}>
        lock
      </button>
    </div>
  );
}

describe('OperatorKeyProvider', () => {
  it('asks for the key once, keeps it only in memory, and forgets it on lock', async () => {
    const user = userEvent.setup();
    const setItem = vi.spyOn(Storage.prototype, 'setItem');
    render(
      <OperatorKeyProvider>
        <Probe />
      </OperatorKeyProvider>,
    );

    await user.click(screen.getByRole('button', { name: 'act' }));
    const input = screen.getByLabelText('Operator key');
    expect(input).toHaveAttribute('type', 'password');
    expect(input).toHaveAttribute('autocomplete', 'off');
    await user.type(input, 's3cret');
    await user.click(screen.getByRole('button', { name: 'Unlock' }));
    expect(screen.getByText('got: s3cret')).toBeInTheDocument();
    expect(screen.getByText('state: unlocked')).toBeInTheDocument();

    // A second action reuses the key without asking.
    await user.click(screen.getByRole('button', { name: 'act' }));
    expect(screen.queryByLabelText('Operator key')).not.toBeInTheDocument();

    expect(setItem).not.toHaveBeenCalled();
    expect(document.cookie).toBe('');
    expect(window.location.href).not.toContain('s3cret');

    await user.click(screen.getByRole('button', { name: 'lock' }));
    expect(screen.getByText('state: locked')).toBeInTheDocument();
  });

  it('resolves null when the unlock dialog is cancelled', async () => {
    const user = userEvent.setup();
    render(
      <OperatorKeyProvider>
        <Probe />
      </OperatorKeyProvider>,
    );
    await user.click(screen.getByRole('button', { name: 'act' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.getByText('got: cancelled')).toBeInTheDocument();
    expect(screen.getByText('state: locked')).toBeInTheDocument();
  });

  it('locks itself after the inactivity timeout', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
      render(
        <OperatorKeyProvider autoLockMs={1_000}>
          <Probe />
        </OperatorKeyProvider>,
      );
      await user.click(screen.getByRole('button', { name: 'act' }));
      await user.type(screen.getByLabelText('Operator key'), 'k');
      await user.click(screen.getByRole('button', { name: 'Unlock' }));
      expect(screen.getByText('state: unlocked')).toBeInTheDocument();
      act(() => vi.advanceTimersByTime(1_500));
      expect(screen.getByText('state: locked')).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });
});
