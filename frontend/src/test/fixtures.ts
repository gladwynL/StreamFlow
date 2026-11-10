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
