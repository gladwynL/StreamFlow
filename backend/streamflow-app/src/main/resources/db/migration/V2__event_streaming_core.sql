-- Phase 2: event persistence, deduplication ledger, analytics aggregates and run accounting.

-- Simulation runs and their producer-side accounting -------------------------------------------
CREATE TABLE simulation_runs (
    run_id            uuid        PRIMARY KEY,
    seed              bigint      NOT NULL,
    content_version   integer     NOT NULL,
    requested_events  bigint      NOT NULL CHECK (requested_events > 0),
    rate_per_second   integer     NOT NULL CHECK (rate_per_second > 0),
    users             integer     NOT NULL CHECK (users > 0),
    topic             text        NOT NULL,
    status            text        NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'INCOMPLETE')),
    status_detail     text,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    attempted         bigint      NOT NULL DEFAULT 0 CHECK (attempted >= 0),
    acked             bigint      NOT NULL DEFAULT 0 CHECK (acked >= 0),
    failed_definite   bigint      NOT NULL DEFAULT 0 CHECK (failed_definite >= 0),
    failed_ambiguous  bigint      NOT NULL DEFAULT 0 CHECK (failed_ambiguous >= 0)
);

COMMENT ON TABLE simulation_runs IS
    'One row per run. A run left RUNNING (generator crashed) can never reconcile as PASS.';

-- Failed publications, by sequence number. Acknowledged events are not stored: they are every
-- attempted sequence number that does not appear here.
CREATE TABLE publish_failures (
    run_id     uuid    NOT NULL REFERENCES simulation_runs (run_id),
    sequence   bigint  NOT NULL CHECK (sequence >= 0),
    event_id   uuid    NOT NULL,
    outcome    text    NOT NULL CHECK (outcome IN ('DEFINITE', 'AMBIGUOUS')),
    error      text    NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

-- Deduplication ledger ------------------------------------------------------------------------------
-- The primary key is the idempotency guarantee: an event's effects are written in the same transaction
-- as its ledger row, so a redelivered event conflicts here and changes nothing else.
CREATE TABLE processed_events (
    event_id        uuid        PRIMARY KEY,
    run_id          uuid        NOT NULL,
    run_seq         bigint      NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    processed_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_run_idx ON processed_events (run_id, run_seq);
CREATE INDEX processed_events_processed_at_brin ON processed_events USING brin (processed_at);

-- Processed events, partitioned by day of ingestion ---------------------------------------------------
CREATE TABLE events (
    event_id        uuid        NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    event_type      text        NOT NULL CHECK (event_type IN ('USER_LOGIN', 'PRODUCT_VIEWED', 'CART_UPDATED',
                                                               'PURCHASE_COMPLETED', 'PAYMENT_FAILED',
                                                               'APPLICATION_ERROR')),
    occurred_at     timestamptz NOT NULL,
    kafka_ts        timestamptz NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    run_id          uuid        NOT NULL,
    run_seq         bigint      NOT NULL,
    user_id         text        NOT NULL,
    session_id      text        NOT NULL,
    synthetic       boolean     NOT NULL,
    amount_cents    bigint,
    currency        text,
    payload         jsonb       NOT NULL,
    PRIMARY KEY (event_id, ingested_at)
) PARTITION BY RANGE (ingested_at);

CREATE INDEX events_ingested_at_idx ON events (ingested_at DESC);
CREATE INDEX events_type_ingested_at_idx ON events (event_type, ingested_at DESC);
CREATE INDEX events_run_idx ON events (run_id);

-- Catches rows outside every daily partition so an insert never fails; maintenance keeps it empty.
CREATE TABLE events_default PARTITION OF events DEFAULT;

-- Creates missing daily partitions (UTC days) from days_back before today to days_ahead after it.
-- Returns the number created. Safe to call repeatedly and concurrently.
CREATE FUNCTION ensure_event_partitions(days_back integer, days_ahead integer) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    today   date := (now() AT TIME ZONE 'UTC')::date;
    day     date;
    name    text;
    created integer := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('ensure_event_partitions'));
    FOR day IN SELECT generate_series(today - days_back, today + days_ahead, interval '1 day')::date LOOP
        name := format('events_%s', to_char(day, 'YYYYMMDD'));
        IF to_regclass(name) IS NULL THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
                           name,
                           day::timestamp AT TIME ZONE 'UTC',
                           (day + 1)::timestamp AT TIME ZONE 'UTC');
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END
$$;

SELECT ensure_event_partitions(1, 7);

-- Analytics aggregates ------------------------------------------------------------------------------
-- Keyed by Kafka partition: a partition has one consumer at a time, so workers never contend for a row.
CREATE TABLE event_stats_minute (
    bucket_start    timestamptz NOT NULL,
    event_type      text        NOT NULL,
    kafka_partition integer     NOT NULL,
    event_count     bigint      NOT NULL CHECK (event_count >= 0),
    amount_cents    bigint      NOT NULL CHECK (amount_cents >= 0),
    PRIMARY KEY (bucket_start, event_type, kafka_partition)
);

-- Per-run processing statistics, updated in the same transaction as the ledger. duplicates_suppressed
-- counts deliveries, not events: one event redelivered twice adds two.
CREATE TABLE run_stats (
    run_id                uuid    NOT NULL,
    kafka_partition       integer NOT NULL,
    processed             bigint  NOT NULL DEFAULT 0 CHECK (processed >= 0),
    duplicates_suppressed bigint  NOT NULL DEFAULT 0 CHECK (duplicates_suppressed >= 0),
    PRIMARY KEY (run_id, kafka_partition)
);

-- Reconciliation results, kept after the ledger rows they were computed from expire.
CREATE TABLE run_reconciliations (
    id         bigserial   PRIMARY KEY,
    run_id     uuid        NOT NULL REFERENCES simulation_runs (run_id),
    checked_at timestamptz NOT NULL DEFAULT now(),
    result     text        NOT NULL CHECK (result IN ('PASS', 'FAIL', 'INCOMPLETE')),
    details    jsonb       NOT NULL
);

CREATE INDEX run_reconciliations_run_idx ON run_reconciliations (run_id, checked_at DESC);
