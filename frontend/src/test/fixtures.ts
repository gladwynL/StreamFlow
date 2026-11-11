import type { LiveSnapshot } from '../api/types';
import type { SystemStatus } from '../api/systemStatus';

/** A healthy status response; tests override the parts they care about. */
export function healthyStatus(overrides: Partial<SystemStatus> = {}): SystemStatus {
  return {
    status: 'OK',
    checkedAt: '2026-11-10T19:00:05Z',
    issues: [],
    kafka: {
      reachable: true,
      clusterId: 'c3RyZWFtZmxvdy1sb2NhbA',
      brokerCount: 1,
      topics: [
        {
          name: 'streamflow.events.v1',
          present: true,
          partitions: 6,
          expectedPartitions: 6,
          replicationFactor: 1,
          configDrift: [],
        },
        {
          name: 'streamflow.events.v1.dlt',
          present: true,
          partitions: 6,
          expectedPartitions: 6,
          replicationFactor: 1,
          configDrift: [],
        },
      ],
      error: null,
    },
    database: { reachable: true, serverVersion: '18.6', schemaVersion: '1', error: null },
    workers: [
      {
        workerId: '5f0c2a9e-1111-4c2e-9a1b-0f6a7b8c9d0e',
        host: 'worker-1',
        startedAt: '2026-11-10T18:50:00Z',
        lastSeenAt: '2026-11-10T19:00:02Z',
        live: true,
      },
    ],
    ...overrides,
  };
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

/** A live snapshot with every value measured; tests override what they assert on. */
export function liveSnapshot(overrides: Partial<LiveSnapshot> = {}): LiveSnapshot {
  return {
    sequence: 42,
    at: '2026-11-11T10:00:00Z',
    throughput: {
      publishedPerSecond: 1012.5,
      processedPerSecond: 987.3,
      processedLastMinute: 59_100,
      deadLetteredLastMinute: 12,
      failedAttemptsPerSecond: { BAD_RECORD: 0.2, TRANSIENT: 1.5, INFRASTRUCTURE: 0 },
      duplicatesPerSecond: 0.4,
    },
    latency: { samples: 290_000, p50Millis: 15, p95Millis: 70, p99Millis: 150 },
    kafka: {
      groupState: 'STABLE',
      members: 3,
      totalLag: 321,
      logEndOffset: 1_000_000,
      partitions: [
        {
          partition: 0,
          logEndOffset: 500_000,
          committedOffset: 499_800,
          lag: 200,
          owner: 'worker-a-0',
        },
        { partition: 1, logEndOffset: 500_000, committedOffset: 499_879, lag: 121, owner: null },
      ],
      error: null,
    },
    databaseError: null,
    workers: {
      live: 1,
      seen: 2,
      anyInOutage: false,
      list: [
        {
          workerId: '0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b',
          host: 'worker-a',
          live: true,
          lastSeenAt: '2026-11-11T09:59:59Z',
          processed: 123_456,
          duplicates: 7,
          failedAttempts: 30,
          deadLettered: 12,
          inOutage: false,
          partitions: [0, 1],
        },
      ],
    },
    deadLetters: { PENDING: 5, REPLAY_REQUESTED: 0, REPLAYED: 2, DISCARDED: 1, EXPIRED: 0 },
    activeRun: null,
    recentEvents: [
      {
        eventId: '0199a1b2-0000-7000-8000-000000000001',
        eventType: 'PURCHASE_COMPLETED',
        occurredAt: '2026-11-11T09:59:59.100Z',
        kafkaTimestamp: '2026-11-11T09:59:59.200Z',
        ingestedAt: '2026-11-11T09:59:59.210Z',
        kafkaPartition: 1,
        kafkaOffset: 499_878,
        runId: '0199a1b2-aaaa-7000-8000-000000000000',
        runSeq: 9,
        userId: 'u-1',
        sessionId: 's-1',
        amountCents: 2599,
        currency: 'USD',
        latencyMillis: 10,
        replayed: false,
      },
    ],
    recentDeadLetters: [
      {
        id: 77,
        eventId: '0199a1b2-0000-7000-8000-000000000002',
        failureCategory: 'BAD_RECORD',
        errorMessage: 'Rejected by fault scenario',
        sourceTopic: 'streamflow.events.v1',
        sourcePartition: 0,
        sourceOffset: 12,
        deadLetteredAt: '2026-11-11T09:59:58Z',
        status: 'PENDING',
      },
    ],
    ...overrides,
  };
}
