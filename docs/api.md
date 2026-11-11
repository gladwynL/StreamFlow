# API reference

Served by the `api` role on port 8080 (in Docker: through nginx at `http://localhost:8080`). Errors use RFC 9457
problem details (`application/problem+json`). Every published port binds to 127.0.0.1; there is no user
authentication — this is a local deployment (ADR-0012).

## Security model for operator actions

Replay and discard change state, so they additionally require:

- the operator key in the `X-StreamFlow-Operator-Key` header, compared in constant time with
  `STREAMFLOW_API_OPERATOR_KEY` (set `STREAMFLOW_OPERATOR_KEY` in `.env` for Docker Compose);
- at most 30 operator requests per minute per api process (`streamflow.api.operator-requests-per-minute`).

| Situation | Response |
|---|---|
| No operator key configured | `503 Service Unavailable` — operator actions are disabled |
| Missing or wrong key | `401 Unauthorized` |
| Rate limit exceeded | `429 Too Many Requests` |

Read-only endpoints need no key.

## `GET /api/v1/system/status`

Live checks of Kafka, PostgreSQL and worker heartbeats, plus dead-letter counts by status. Always `200`; a degraded
dependency is reported in the body (`status: DEGRADED`, `issues: [...]`). Dead-letter counts are informational and
do not affect `status`.

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

## Examples

```bash
curl -s "http://localhost:8080/api/v1/dead-letters?status=PENDING&limit=10"
curl -s http://localhost:8080/api/v1/dead-letters/12
curl -s -X POST http://localhost:8080/api/v1/dead-letters/replay \
  -H "Content-Type: application/json" -H "X-StreamFlow-Operator-Key: $STREAMFLOW_OPERATOR_KEY" \
  -d '{"ids":[12]}'
```
