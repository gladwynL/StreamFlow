import { useEffect, useRef, useState } from 'react';
import type { LiveSnapshot } from '../api/types';
import { appendTrend, type TrendPoint } from './trend';

/**
 * - connecting: first connection attempt
 * - live: snapshots arriving
 * - stale: connected, but no snapshot for STALE_AFTER_MS
 * - reconnecting: connection lost; retrying with backoff
 */
export type FeedStatus = 'connecting' | 'live' | 'stale' | 'reconnecting';

export interface LiveFeed {
  snapshot: LiveSnapshot | null;
  status: FeedStatus;
  /** When the last snapshot arrived (client clock), or null. */
  receivedAt: number | null;
  reconnects: number;
  /** Snapshots received since the page opened (bounded), for trends. */
  trend: TrendPoint[];
}

export const STALE_AFTER_MS = 4_000;
export const DEAD_AFTER_MS = 10_000;
export const PING_EVERY_MS = 15_000;
const BACKOFF_MS = [1_000, 2_000, 4_000, 8_000, 15_000];

export interface FeedOptions {
  url?: string;
  /** Fetches a fresh snapshot over REST; called after every (re)connection to resynchronise. */
  resync: (signal: AbortSignal) => Promise<LiveSnapshot>;
  createSocket?: (url: string) => WebSocket;
}

export function liveUrl(location: Location = window.location): string {
  const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${location.host}/ws/v1/live`;
}

/**
 * One WebSocket to the live feed. Reconnects with capped exponential backoff and jitter; after each connection it
 * fetches a REST snapshot so the view is current even before the next push; flags the feed stale when pushes stop
 * and drops a silent connection; pings so idle proxies keep it open; and releases everything on unmount.
 * Snapshots older than the one shown (by sequence) are ignored.
 */
export function useLiveFeed({ url, resync, createSocket }: FeedOptions): LiveFeed {
  const [feed, setFeed] = useState<LiveFeed>({
    snapshot: null,
    status: 'connecting',
    receivedAt: null,
    reconnects: 0,
    trend: [],
  });
  const resyncRef = useRef(resync);
  useEffect(() => {
    resyncRef.current = resync;
  }, [resync]);

  useEffect(() => {
    const target = url ?? liveUrl();
    const open = createSocket ?? ((u: string) => new WebSocket(u));
    let socket: WebSocket | null = null;
    let disposed = false;
    let attempt = 0;
    let lastMessageAt = 0;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let resyncAbort: AbortController | null = null;

    const accept = (snapshot: LiveSnapshot) => {
      lastMessageAt = Date.now();
      setFeed((current) => {
        if (current.snapshot && snapshot.sequence < current.snapshot.sequence) {
          return current.status === 'live' ? current : { ...current, status: 'live' };
        }
        return {
          ...current,
          snapshot,
          status: 'live',
          receivedAt: lastMessageAt,
          trend: appendTrend(current.trend, snapshot),
        };
      });
    };

    const scheduleReconnect = () => {
      if (disposed) {
        return;
      }
      const base = BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)] ?? 15_000;
      attempt += 1;
      const delay = base * (0.8 + Math.random() * 0.4);
      setFeed((current) => ({
        ...current,
        status: 'reconnecting',
        reconnects: current.reconnects + 1,
      }));
      reconnectTimer = setTimeout(connect, delay);
    };

    function connect() {
      if (disposed) {
        return;
      }
      let ws: WebSocket;
      try {
        ws = open(target);
      } catch {
        scheduleReconnect();
        return;
      }
      socket = ws;
      ws.onopen = () => {
        attempt = 0;
        lastMessageAt = Date.now();
        resyncAbort?.abort();
        resyncAbort = new AbortController();
        resyncRef
          .current(resyncAbort.signal)
          .then((snapshot) => {
            if (!disposed) {
              accept(snapshot);
            }
          })
          .catch(() => {
            // The next push brings the state anyway.
          });
      };
      ws.onmessage = (message: MessageEvent) => {
        if (typeof message.data !== 'string') {
          return;
        }
        try {
          const parsed = JSON.parse(message.data) as { type?: string; data?: LiveSnapshot };
          if (parsed.type === 'snapshot' && parsed.data) {
            accept(parsed.data);
          }
        } catch {
          // Ignore malformed frames.
        }
      };
      ws.onclose = () => {
        if (socket === ws) {
          socket = null;
          scheduleReconnect();
        }
      };
      ws.onerror = () => {
        // onclose follows and schedules the reconnect.
      };
    }

    const watchdog = setInterval(() => {
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        return;
      }
      const silentFor = Date.now() - lastMessageAt;
      if (silentFor > DEAD_AFTER_MS) {
        const dead = socket;
        socket = null;
        dead.close(4000, 'stale');
        scheduleReconnect();
      } else if (silentFor > STALE_AFTER_MS) {
        setFeed((current) =>
          current.status === 'stale' ? current : { ...current, status: 'stale' },
        );
      }
    }, 1_000);

    const pinger = setInterval(() => {
      if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send('ping');
      }
    }, PING_EVERY_MS);

    connect();

    return () => {
      disposed = true;
      clearInterval(watchdog);
      clearInterval(pinger);
      clearTimeout(reconnectTimer);
      resyncAbort?.abort();
      if (socket) {
        const closing = socket;
        socket = null;
        closing.onclose = null;
        closing.close(1000, 'disposed');
      }
    };
  }, [url, createSocket]);

  return feed;
}
