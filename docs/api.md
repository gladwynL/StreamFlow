# API reference

Machine-readable description: [`openapi.yaml`](openapi.yaml) (OpenAPI 3.1, checked against the controllers by
`OpenApiCoverageTest`).

Served by the `api` role on port 8080 (in Docker: through nginx at `http://localhost:8080`). Errors use RFC 9457
problem details (`application/problem+json`). Every published port binds to 127.0.0.1; there is no user
authentication — this is a local deployment (ADR-0012).

## Security model for operator actions

Replay, discard and the simulation actions (start, stop, reconcile) change state, so they additionally require:

- the operator key in the `X-StreamFlow-Operator-Key` header, compared in constant time with
  `STREAMFLOW_API_OPERATOR_KEY` (set `STREAMFLOW_OPERATOR_KEY` in `.env` for Docker Compose);
- at most 30 operator requests per minute per api process (`streamflow.api.operator-requests-per-minute`); rejected
  attempts count too, so the key cannot be guessed faster than that.

| Situation | Response |
|---|---|
| No operator key configured | `503 Service Unavailable` — operator actions are disabled |
| Missing or wrong key | `401 Unauthorized` |
| Rate limit exceeded | `429 Too Many Requests` |

Read-only endpoints need no key. The dashboard keeps the key in the tab's memory only (never in a URL, storage,
cookie or log), sends it only in the header above, and forgets it after 10 minutes without operator activity.

## Conventions

- All values are measurements from PostgreSQL or Kafka. A value that could not be measured is `null`, never 0.
- Times are ISO-8601 UTC. Latency is from the Kafka append timestamp to the start of the processing transaction.
- Latency percentiles come from a fixed histogram (buckets 1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200,
  300, 500, 700 ms, 1, 1.5, 2, 3, 5, 7, 10, 15, 30, 60 s, open-ended); each percentile is the **upper bound** of the
  bucket containing it, so it never understates latency.
- Publication, processing, failures and duplicates are separate fields everywhere; none is derived from another.
- Page sizes are capped at 100. Every list query is bounded by a time window, a limit or a keyset cursor and uses an
  index.

## `GET /api/v1/system/status`

Live checks of Kafka, PostgreSQL and worker heartbeats, plus dead-letter counts by status. Always `200`; a degraded
dependency is reported in the body (`status: DEGRADED`, `issues: [...]`). Dead-letter counts are informational and
do not affect `status`.

Phase 6 additions: `offsetGaps` lists recorded offset gaps (`consumerGroup`, `topic`, `partition`, `lostPosition`,
`logStartOffset`, `eventsLost`, `detectedAt`, `occurrences`, `open`, `committedOffset`); an open gap is an issue with
the operator procedure (runbook, "Offset gaps"). A partition with unread records at 80 % or more of its
`retention.bytes` is also reported as an issue.

## `GET /api/v1/dead-letters`

Newest first. Query parameters:

| Parameter | Meaning |
|---|---|
| `status` | optional: `PENDING`, `REPLAY_REQUESTED`, `REPLAYED`, `DISCARDED`, `EXPIRED` |
| `before` | optional: return dead letters with an id lower than this (paging) |
| `limit` | 1–100, default 50 |

Each item: `id`, `status`, `failureCategory` (`BAD_RECORD`, `TRANSIENT`, `UNKNOWN`), `errorMessage`, `eventId`,
`runId`, `runSeq` (null when the record carried no identity headers), `sourceTopic`, `sourcePartition`,
`sourceOffset`, `deadLetteredAt`, `replayCount`.

## `GET /api/v1/dead-letters/{id}`

One dead letter with its provenance: the summary fields above plus `exceptionClass`, `attempts`, `recordKey`,
`payloadBytes`, the first 4 KiB of the payload as `payloadText` (valid UTF-8) or `payloadBase64` (otherwise),
`headers` (original `sf-*` and `kafka_dlt-*` headers; stack traces are not stored), `statusChangedAt`, `replayedAt`,
`discardReason`. `404` if unknown.

## `POST /api/v1/dead-letters/replay` (operator)

```json
{ "ids": [12, 13, 14] }
```

1–100 positive ids. For each id, the dead letter is claimed (`PENDING → REPLAY_REQUESTED`), its original key, value
and identity headers are republished to the source topic with `sf-replay-of: <id>`, and after the broker
acknowledges it the dead letter becomes `REPLAYED`. Returns one result per distinct id:

| `outcome` | Meaning |
|---|---|
| `REPLAYED` | published and marked REPLAYED |
| `NOT_FOUND` | no such dead letter |
| `NOT_PENDING` | already replayed, discarded, expired, or claimed by a concurrent replay |
| `OUTSIDE_REPLAY_WINDOW` | dead-lettered more than 7 days ago; replaying could outlive deduplication |
| `NO_EVENT_ID` | the record has no event id (undecodable without identity headers); discard it instead |
| `PUBLISH_FAILED` | publication failed; the dead letter is PENDING again (`detail` has the error) |

Replaying is idempotent in effect: the replayed record carries the original event id, so the processing ledger
suppresses any second delivery. Replayed events are processed after events published while they were dead-lettered;
per-key order is not preserved across a dead-letter and replay.

## `POST /api/v1/dead-letters/{id}/discard` (operator)

```json
{ "reason": "duplicate of order o-1a2b, handled manually" }
```

`reason` is required (1–200 characters). Only `PENDING` or `EXPIRED` dead letters can be discarded (`409`
otherwise, `404` if unknown). Discarding is final and is counted as `discarded` by run reconciliation.

## Live feed

### `WebSocket /ws/v1/live`

Pushes `{"type":"snapshot","data":<LiveSnapshot>}` about once per second while at least one client is connected (no
per-event messages). Only `localhost` and `127.0.0.1` origins (any port) may connect. Clients may send small text
messages (the dashboard sends `ping` every 15 s); their content is ignored. Server limits:

| Limit | Default | Setting |
|---|---|---|
| Concurrent clients; further connections are closed with `1013` | 20 | `streamflow.api.max-live-clients` |
| A send slower than this closes the client (`4008 client too slow`) | 3 s | `streamflow.api.live-send-timeout` |
| Consecutive snapshots a client may miss (previous send still in progress) before it is closed | 5 | `streamflow.api.live-max-skipped-ticks` |
| Buffered outbound data per client | 1 MiB | fixed |
| Inbound message size | 4 KiB | fixed |
| Idle connection (nothing received) | 45 s | fixed |

The snapshot is computed once per tick, whatever the number of clients, and serialized once. Sends run on a bounded
pool; each client has at most one snapshot in flight, so a slow browser skips snapshots instead of queuing them. The
feed runs in the api process; Kafka processing runs in the workers and never waits on it.

### `GET /api/v1/live`

The same `LiveSnapshot` on demand (the dashboard fetches it after every (re)connection). Snapshots are cached for up
to 900 ms, so REST and WebSocket share one computation per second.

| Field | Meaning |
|---|---|
| `sequence`, `at` | increases by one per snapshot of this api process; computation time |
| `throughput.publishedPerSecond` | growth of the event topic's log-end offsets between snapshots (null on the first) |
| `throughput.processedPerSecond` | events applied (first deliveries) in the last 10 s, divided by 10 |
| `throughput.processedLastMinute`, `deadLetteredLastMinute` | counts over the last 60 s |
| `throughput.failedAttemptsPerSecond` | failed processing attempts per category (`BAD_RECORD`, `TRANSIENT`, `INFRASTRUCTURE`) across live workers over about 10 s, from heartbeat counters; includes attempts that later succeeded |
| `throughput.duplicatesPerSecond` | deliveries suppressed by the ledger across live workers over about 10 s |
| `latency` | `samples`, `p50Millis`, `p95Millis`, `p99Millis` over the last 5 minutes |
| `kafka` | processing group `groupState`, `members`, `totalLag`, `logEndOffset`, `retentionBytes` (size limit per partition), and per partition `logEndOffset`, `committedOffset`, `lag`, `owner` (client id from the group coordinator), `sizeBytes` (stored on the broker); `error` when Kafka could not be queried |
| `databaseError` | set when PostgreSQL could not be queried; throughput, latency, workers and recent rows are then null |
| `workers` | `live`, `seen`, `anyInOutage`, and per worker its counters since process start and assigned partitions |
| `deadLetters` | counts by status |
| `activeRun` | the run started from this api process (see Simulations), or null |
| `recentEvents`, `recentDeadLetters` | the 20 newest processed events and 10 newest dead letters |

Rates derived from worker counters only subtract counters of the same worker process (a restarted worker has a new
id), so restarts never produce negative or inflated rates.

## Events

### `GET /api/v1/events`

Processed events, newest first by processing time. Parameters: `type` (event type), `runId`, `limit` (1-100,
default 50), `cursor` (opaque, from `nextCursor`). Returns `{ "items": [...], "nextCursor": "..." | null }`. Keyset
paging on `(ingestedAt, eventId)`: no gaps or repeats while new events arrive. Each item: `eventId`, `eventType`,
`occurredAt`, `kafkaTimestamp`, `ingestedAt`, `kafkaPartition`, `kafkaOffset`, `runId`, `runSeq`, `userId`,
`sessionId`, `amountCents`, `currency`, `latencyMillis`, `replayed` (processed after a dead-letter replay).

### `GET /api/v1/events/{eventId}`

One event with its `payload` and the dead letters recorded for it (`deadLetters`). `404` if it was not processed.

## Analytics

### `GET /api/v1/analytics/series?range=15m|1h|6h|24h`

Per-minute `points` (`minute`, `processed`, `processedPerSecond`, `amountCents`, `deadLettered`, `latencySamples`,
`p50Millis`, `p95Millis`, `p99Millis`; minutes without activity are omitted) and a `latency` summary over the range
with the full `histogram` (`upperBoundMillis`, `count`; the last bucket has no upper bound).

### `GET /api/v1/analytics/distribution?range=...` or `?runId=...`

Processed events and amounts by event type, over a range (default `1h`) or for one run (not both).

## Kafka and workers

### `GET /api/v1/kafka/consumer-groups`

The `processing` (event topic) and `deadLetterIngestion` (dead-letter topic) groups as Kafka reports them: `state`,
`members` (client id, host, assigned partitions) and per-partition offsets, lag and owner. `503` if Kafka cannot be
queried.

### `GET /api/v1/workers`

Workers seen in the last 10 minutes with `live`, `startedAt`, `lastSeenAt` and `counters` (`processed`,
`duplicates`, `failuresBadRecord`, `failuresTransient`, `failuresInfrastructure`, `deadLettered`,
`deadLetterFailures`, `rebalances`, `consumerErrors`, `inOutage`, `assignedPartitions`). Counters restart at 0 with
the worker process.

## Simulations

All runs are synthetic traffic from the deterministic generator.

### `GET /api/v1/simulations?before=...&limit=...`

Runs newest first (`before`: a `startedAt` for paging; `limit` 1-100, default 20). Each run keeps publication
(`attempted`, `acked`, `failedDefinite`, `failedAmbiguous`), processing (`processed`), `duplicatesSuppressed`,
`deadLetters` and the latest `reconciliationResult` separate, with its configuration (`seed`, `requestedEvents`,
`ratePerSecond`, `users`, `eventTypes`, null meaning every type).

### `GET /api/v1/simulations/{runId}`

`{ run, distribution, reconciliation }`, where `reconciliation` is the latest stored report.

### `GET /api/v1/simulations/active`

Progress of the run started from this api process: `phase` (`PUBLISHING`; `DRAINING`, waiting for zero lag in both
consumer groups before reconciling; or `RECONCILED`), `attempted`, `acked`, `failedDefinite`, `failedAmbiguous`,
`error`. `204` if there is none.

### `POST /api/v1/simulations` (operator)

```json
{ "seed": 42, "events": 20000, "ratePerSecond": 1000, "users": 10000,
  "eventTypes": ["PURCHASE_COMPLETED", "PAYMENT_FAILED"] }
```

Give `events` or `durationSeconds` (events = rate x duration). Bounds: events 1-500,000; rate 1-5,000/s; at most
600 s at the requested rate; users 1-100,000 (default 10,000). `seed` is optional (random if omitted, and returned);
`eventTypes` is optional (all types). Returns `202` with the run id. One run at a time per api process: `409` while
a run is publishing or draining. `400` for a request outside the bounds.

### `POST /api/v1/simulations/active/stop` (operator)

Stops publishing after the current event (`202`); the run then drains and is reconciled against what was actually
published. `409` if no run is publishing.

### `POST /api/v1/simulations/{runId}/reconcile` (operator)

Re-runs reconciliation now and stores the result. `404` for an unknown run; `409` while that run is in progress;
`503` if Kafka does not answer the lag query (try again).

## Examples

```bash
curl -s "http://localhost:8080/api/v1/dead-letters?status=PENDING&limit=10"
curl -s http://localhost:8080/api/v1/dead-letters/12
curl -s -X POST http://localhost:8080/api/v1/dead-letters/replay \
  -H "Content-Type: application/json" -H "X-StreamFlow-Operator-Key: $STREAMFLOW_OPERATOR_KEY" \
  -d '{"ids":[12]}'
curl -s "http://localhost:8080/api/v1/events?type=PURCHASE_COMPLETED&limit=20"
curl -s "http://localhost:8080/api/v1/analytics/series?range=1h"
curl -s -X POST http://localhost:8080/api/v1/simulations \
  -H "Content-Type: application/json" -H "X-StreamFlow-Operator-Key: $STREAMFLOW_OPERATOR_KEY" \
  -d '{"events":20000,"ratePerSecond":1000}'
```
