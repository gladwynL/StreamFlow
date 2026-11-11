-- Phase 4: measurements the dashboard reads. Every value is written by the component that observes it.

-- Processing latency histogram ---------------------------------------------------------------------------
-- Latency = database transaction start − Kafka append timestamp, in milliseconds, counted into fixed
-- log-spaced buckets per minute and partition, in the same transaction as the event. Bucket counts merge by
-- addition, so percentiles over any time range and any set of partitions are cheap and exact to the bucket.
CREATE TABLE latency_stats_minute (
    bucket_start    timestamptz NOT NULL,
    kafka_partition integer     NOT NULL,
    bucket_index    smallint    NOT NULL CHECK (bucket_index >= 0),
    event_count     bigint      NOT NULL CHECK (event_count >= 0),
    PRIMARY KEY (bucket_start, kafka_partition, bucket_index)
);

-- Worker counters ---------------------------------------------------------------------------------------
-- Process-lifetime counters each worker reports with its heartbeat (they restart at zero with the process).
ALTER TABLE worker_heartbeats
    ADD COLUMN processed             bigint  NOT NULL DEFAULT 0,
    ADD COLUMN duplicates            bigint  NOT NULL DEFAULT 0,
    ADD COLUMN failures_bad_record   bigint  NOT NULL DEFAULT 0,
    ADD COLUMN failures_transient    bigint  NOT NULL DEFAULT 0,
    ADD COLUMN failures_infra        bigint  NOT NULL DEFAULT 0,
    ADD COLUMN dead_lettered         bigint  NOT NULL DEFAULT 0,
    ADD COLUMN dead_letter_failures  bigint  NOT NULL DEFAULT 0,
    ADD COLUMN rebalances            bigint  NOT NULL DEFAULT 0,
    ADD COLUMN consumer_errors       bigint  NOT NULL DEFAULT 0,
    ADD COLUMN in_outage             boolean NOT NULL DEFAULT false,
    ADD COLUMN assigned_partitions   integer[] NOT NULL DEFAULT '{}';

-- Dead letters by event (event detail, live stream status) ---------------------------------------------
CREATE INDEX dead_letter_events_event_idx ON dead_letter_events (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX dead_letter_events_dead_lettered_at_idx ON dead_letter_events (dead_lettered_at DESC);

-- Event types a run was restricted to (NULL: every type) ------------------------------------------------
ALTER TABLE simulation_runs ADD COLUMN event_types text[];
