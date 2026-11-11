import { act, renderHook } from '@testing-library/react';
import type { LiveSnapshot } from '../api/types';
import { liveSnapshot } from '../test/fixtures';
import { DEAD_AFTER_MS, STALE_AFTER_MS, useLiveFeed } from './useLiveFeed';

/** A controllable WebSocket stand-in. */
class FakeSocket {
  static instances: FakeSocket[] = [];
  readyState = 0;
  sent: string[] = [];
  closed: { code?: number; reason?: string } | null = null;
  onopen: (() => void) | null = null;
  onmessage: ((message: MessageEvent) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;

  readonly url: string;

  constructor(url: string) {
    this.url = url;
    FakeSocket.instances.push(this);
  }

  open() {
    this.readyState = 1;
    this.onopen?.();
  }

  push(snapshot: LiveSnapshot) {
    this.onmessage?.({
      data: JSON.stringify({ type: 'snapshot', data: snapshot }),
    } as MessageEvent);
  }

  drop() {
    this.readyState = 3;
    this.onclose?.();
  }

  send(data: string) {
    this.sent.push(data);
  }

  close(code?: number, reason?: string) {
    this.readyState = 3;
    this.closed = { code, reason };
  }
}

const createSocket = (url: string) => new FakeSocket(url) as unknown as WebSocket;

function setup(resync = vi.fn().mockResolvedValue(liveSnapshot({ sequence: 1 }))) {
  const hook = renderHook(() => useLiveFeed({ url: 'ws://test/ws/v1/live', resync, createSocket }));
  return { hook, resync, socket: () => FakeSocket.instances[FakeSocket.instances.length - 1]! };
}

describe('useLiveFeed', () => {
  beforeEach(() => {
    FakeSocket.instances = [];
    vi.useFakeTimers();
    vi.stubGlobal('WebSocket', { OPEN: 1 });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('resynchronises over REST on connect, then applies pushed snapshots in sequence order', async () => {
    const { hook, resync, socket } = setup();
    expect(hook.result.current.status).toBe('connecting');

    await act(async () => socket().open());
    expect(resync).toHaveBeenCalledTimes(1);
    expect(hook.result.current.snapshot?.sequence).toBe(1);
    expect(hook.result.current.status).toBe('live');

    act(() => socket().push(liveSnapshot({ sequence: 5 })));
    expect(hook.result.current.snapshot?.sequence).toBe(5);
    act(() => socket().push(liveSnapshot({ sequence: 4 })));
    expect(hook.result.current.snapshot?.sequence).toBe(5);
    expect(hook.result.current.trend.map((p) => p.sequence)).toEqual([1, 5]);
  });

  it('flags a silent feed as stale, then drops it and reconnects with a fresh REST snapshot', async () => {
    const { hook, resync, socket } = setup();
    await act(async () => socket().open());
    const first = socket();

    act(() => vi.advanceTimersByTime(STALE_AFTER_MS + 1_000));
    expect(hook.result.current.status).toBe('stale');

    act(() => vi.advanceTimersByTime(DEAD_AFTER_MS));
    expect(first.closed?.code).toBe(4000);
    expect(hook.result.current.status).toBe('reconnecting');

    act(() => vi.advanceTimersByTime(1_500));
    expect(FakeSocket.instances).toHaveLength(2);
    await act(async () => socket().open());
    expect(resync).toHaveBeenCalledTimes(2);
    expect(hook.result.current.status).toBe('live');
    expect(hook.result.current.reconnects).toBe(1);
  });

  it('backs off between failed reconnects and keeps the last snapshot meanwhile', async () => {
    const { hook, socket } = setup();
    await act(async () => socket().open());
    act(() => socket().drop());
    expect(hook.result.current.status).toBe('reconnecting');
    expect(hook.result.current.snapshot?.sequence).toBe(1);

    act(() => vi.advanceTimersByTime(1_300)); // first retry ≤ 1.2 s
    expect(FakeSocket.instances).toHaveLength(2);
    act(() => socket().drop());
    act(() => vi.advanceTimersByTime(1_500)); // second retry waits ≥ 1.6 s
    expect(FakeSocket.instances).toHaveLength(2);
    act(() => vi.advanceTimersByTime(1_000));
    expect(FakeSocket.instances).toHaveLength(3);
  });

  it('pings while connected and releases the socket and timers on unmount', async () => {
    const { hook, socket } = setup();
    await act(async () => socket().open());
    for (let second = 1; second <= 15; second++) {
      act(() => {
        socket().push(liveSnapshot({ sequence: 1 + second }));
        vi.advanceTimersByTime(1_000);
      });
    }
    expect(socket().sent).toContain('ping');

    hook.unmount();
    expect(socket().closed?.code).toBe(1000);
    act(() => vi.advanceTimersByTime(60_000));
    expect(FakeSocket.instances).toHaveLength(1);
  });
});
