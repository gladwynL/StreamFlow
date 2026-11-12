/** Response types of the StreamFlow API v1 (see docs/api.md). Every number is a backend measurement. */

export const EVENT_TYPES = [
  'USER_LOGIN',
  'PRODUCT_VIEWED',
  'CART_UPDATED',
  'PURCHASE_COMPLETED',
  'PAYMENT_FAILED',
  'APPLICATION_ERROR',
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export const FAILURE_CATEGORIES = ['BAD_RECORD', 'TRANSIENT', 'INFRASTRUCTURE'] as const;

export const DEAD_LETTER_STATUSES = [
  'PENDING',
  'REPLAY_REQUESTED',
  'REPLAYED',
  'DISCARDED',
  'EXPIRED',
] as const;
export type DeadLetterStatus = (typeof DEAD_LETTER_STATUSES)[number];

export interface EventRow {
  eventId: string;
  eventType: string;
  occurredAt: string;
  kafkaTimestamp: string;
  ingestedAt: string;
  kafkaPartition: number;
  kafkaOffset: number;
  runId: string;
  runSeq: number;
  userId: string | null;
  sessionId: string | null;
  amountCents: number | null;
  currency: string | null;
  latencyMillis: number;
  replayed: boolean;
}

export interface DeadLetterRow {
  id: number;
  eventId: string | null;
  failureCategory: string;
  errorMessage: string | null;
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: number;
  deadLetteredAt: string;
  status: DeadLetterStatus;
}

export interface LiveSnapshot {
  sequence: number;
  at: string;
  throughput: Throughput | null;
  latency: Latency | null;
  kafka: KafkaView;
  databaseError: string | null;
  workers: Workers | null;
  deadLetters: Record<string, number> | null;
  activeRun: ActiveRun | null;
  recentEvents: EventRow[] | null;
  recentDeadLetters: DeadLetterRow[] | null;
}

export interface Throughput {
  publishedPerSecond: number | null;
  processedPerSecond: number;
  processedLastMinute: number;
  deadLetteredLastMinute: number;
  failedAttemptsPerSecond: Record<string, number> | null;
  duplicatesPerSecond: number | null;
}

export interface Latency {
  samples: number;
  p50Millis: number | null;
  p95Millis: number | null;
  p99Millis: number | null;
}

export interface KafkaView {
  groupState: string | null;
  members: number | null;
  totalLag: number | null;
  logEndOffset: number | null;
  retentionBytes: number | null;
  partitions: PartitionView[] | null;
  error: string | null;
}

export interface PartitionView {
  partition: number;
  logEndOffset: number;
  committedOffset: number | null;
  lag: number;
  owner: string | null;
  sizeBytes: number | null;
}

export interface Workers {
  live: number;
  seen: number;
  anyInOutage: boolean;
  list: WorkerView[];
}

export interface WorkerView {
  workerId: string;
  host: string;
  live: boolean;
  lastSeenAt: string;
  processed: number;
  duplicates: number;
  failedAttempts: number;
  deadLettered: number;
  inOutage: boolean;
  partitions: number[];
}

export type RunPhase = 'PUBLISHING' | 'DRAINING' | 'RECONCILED';

export interface ActiveRun {
  runId: string | null;
  phase: RunPhase;
  requestedEvents: number;
  ratePerSecond: number;
  attempted: number;
  acked: number;
  failed: number;
  processed: number | null;
  startedAt: string;
}

export interface EventPage {
  items: EventRow[];
  nextCursor: string | null;
}

export interface EventDetail {
  event: EventRow;
  payload: unknown;
  deadLetters: {
    id: number;
    failureCategory: string;
    errorMessage: string | null;
    attempts: number | null;
    deadLetteredAt: string;
    status: DeadLetterStatus;
    replayCount: number;
    replayedAt: string | null;
  }[];
}

export interface SeriesPoint {
  minute: string;
  processed: number;
  processedPerSecond: number;
  amountCents: number;
  deadLettered: number;
  latencySamples: number;
  p50Millis: number | null;
  p95Millis: number | null;
  p99Millis: number | null;
}

export interface Series {
  range: string;
  from: string;
  to: string;
  points: SeriesPoint[];
  latency: {
    samples: number;
    p50Millis: number | null;
    p95Millis: number | null;
    p99Millis: number | null;
    histogram: { upperBoundMillis: number | null; count: number }[];
  };
}

export interface TypeCount {
  eventType: string;
  events: number;
  amountCents: number;
}

export interface Distribution {
  range: string | null;
  runId: string | null;
  types: TypeCount[];
}

export interface GroupState {
  groupId: string;
  topic: string;
  state: string;
  members: { memberId: string; clientId: string; host: string; partitions: number[] }[];
  partitions: {
    partition: number;
    logEndOffset: number;
    committedOffset: number | null;
    lag: number;
    ownerClientId: string | null;
  }[];
}

export interface ConsumerGroups {
  processing: GroupState;
  deadLetterIngestion: GroupState;
}

export interface RunView {
  runId: string;
  seed: number;
  requestedEvents: number;
  ratePerSecond: number;
  users: number;
  eventTypes: string[] | null;
  status: string;
  statusDetail: string | null;
  startedAt: string;
  finishedAt: string | null;
  attempted: number;
  acked: number;
  failedDefinite: number;
  failedAmbiguous: number;
  processed: number;
  duplicatesSuppressed: number;
  deadLetters: number;
  reconciliationResult: string | null;
  reconciledAt: string | null;
}

export interface Reconciliation {
  result: string;
  reasons: string[];
  drained: boolean;
  [key: string]: unknown;
}

export interface RunDetail {
  run: RunView;
  distribution: TypeCount[];
  reconciliation: Reconciliation | null;
}

export interface RunProgress {
  runId: string;
  phase: RunPhase;
  requestedEvents: number;
  ratePerSecond: number;
  attempted: number;
  acked: number;
  failedDefinite: number;
  failedAmbiguous: number;
  startedAt: string;
  error: string | null;
}

export interface StartRequest {
  seed?: number;
  events?: number;
  durationSeconds?: number;
  ratePerSecond: number;
  users?: number;
  eventTypes?: string[];
}

export interface Started {
  runId: string;
  seed: number;
  events: number;
  ratePerSecond: number;
  users: number;
  eventTypes: string[];
}

export interface DeadLetterSummary {
  id: number;
  status: DeadLetterStatus;
  failureCategory: string;
  errorMessage: string | null;
  eventId: string | null;
  runId: string | null;
  runSeq: number | null;
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: number;
  deadLetteredAt: string;
  replayCount: number;
}

export interface DeadLetterDetail {
  summary: DeadLetterSummary;
  exceptionClass: string | null;
  attempts: number | null;
  recordKey: string | null;
  payloadBytes: number;
  payloadText: string | null;
  payloadBase64: string | null;
  headers: Record<string, unknown>;
  statusChangedAt: string | null;
  replayedAt: string | null;
  discardReason: string | null;
}

export type ReplayOutcome =
  'REPLAYED' | 'NOT_FOUND' | 'NOT_PENDING' | 'OUTSIDE_REPLAY_WINDOW' | 'NO_EVENT_ID' | string;

export interface ReplayResult {
  id: number;
  outcome: ReplayOutcome;
  detail: string | null;
}
