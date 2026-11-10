import { getJson } from './client';

/** Mirrors the response of GET /api/v1/system/status. */
export interface SystemStatus {
  status: 'OK' | 'DEGRADED';
  checkedAt: string;
  issues: string[];
  kafka: KafkaStatus;
  database: DatabaseStatus;
  workers: WorkerStatus[];
}

export interface KafkaStatus {
  reachable: boolean;
  clusterId: string | null;
  brokerCount: number | null;
  topics: TopicStatus[];
  error: string | null;
}

export interface TopicStatus {
  name: string;
  present: boolean;
  partitions: number | null;
  expectedPartitions: number;
  replicationFactor: number | null;
  configDrift: ConfigDrift[];
}

export interface ConfigDrift {
  key: string;
  expected: string;
  actual: string | null;
}

export interface DatabaseStatus {
  reachable: boolean;
  serverVersion: string | null;
  schemaVersion: string | null;
  error: string | null;
}

export interface WorkerStatus {
  workerId: string;
  host: string;
  startedAt: string;
  lastSeenAt: string;
  live: boolean;
}

export const systemStatusQueryKey = ['system-status'] as const;

export function fetchSystemStatus(signal?: AbortSignal): Promise<SystemStatus> {
  return getJson<SystemStatus>('/api/v1/system/status', signal);
}
