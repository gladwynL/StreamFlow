-- Phase 3: dead letters, replay lifecycle, fault-injection scenarios and retention helpers.

-- Dead letters ---------------------------------------------------------------------------------------
-- One row per source record that was dead-lettered. The unique source coordinates collapse duplicate
-- dead-letter publications (for example after a crash between the DLT acknowledgement and the source
-- offset commit) into a single row.
CREATE TABLE dead_letter_events (
    id                bigserial   PRIMARY KEY,
    source_topic      text        NOT NULL,
    source_partition  integer     NOT NULL,
    source_offset     bigint      NOT NULL,
    source_timestamp  timestamptz,
    dlt_partition     integer     NOT NULL,
    dlt_offset        bigint      NOT NULL,
    event_id          uuid,                       -- null when the record carried no event id
    run_id            uuid,
    run_seq           bigint,
    record_key        text,
    payload           bytea,                      -- the original value bytes, unmodified
    headers           jsonb       NOT NULL,       -- original sf-* and kafka_dlt-* headers (no stack traces)
    failure_category  text        NOT NULL CHECK (failure_category IN ('BAD_RECORD', 'TRANSIENT', 'UNKNOWN')),
    exception_class   text,
    error_message     text,
    attempts          integer,
    dead_lettered_at  timestamptz NOT NULL,       -- timestamp of the dead-letter record
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    status            text        NOT NULL DEFAULT 'PENDING'
                                  CHECK (status IN ('PENDING', 'REPLAY_REQUESTED', 'REPLAYED', 'DISCARDED', 'EXPIRED')),
    status_changed_at timestamptz NOT NULL DEFAULT now(),
    claim_token       uuid,
    claimed_at        timestamptz,
    replay_count      integer     NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    replayed_at       timestamptz,
    discard_reason    text,
    UNIQUE (source_topic, source_partition, source_offset),
    CHECK ((status = 'REPLAY_REQUESTED') = (claim_token IS NOT NULL))
);

CREATE INDEX dead_letter_events_status_idx ON dead_letter_events (status, dead_lettered_at);
CREATE INDEX dead_letter_events_run_idx ON dead_letter_events (run_id, run_seq);

COMMENT ON TABLE dead_letter_events IS
    'PENDING -> REPLAY_REQUESTED -> REPLAYED, or PENDING -> DISCARDED / EXPIRED. Unresolved rows are never deleted automatically.';

-- Fault-injection scenarios -------------------------------------------------------------------------
-- Only predefined kinds, bounded in count and duration. Workers honour them only when fault injection is
-- enabled in configuration.
CREATE TABLE fault_scenarios (
    id                  bigserial   PRIMARY KEY,
    kind                text        NOT NULL CHECK (kind IN ('REJECT_EVENT_TYPE', 'TRANSIENT_FAILURES')),
    event_type          text        NOT NULL,
    failures_per_record integer     NOT NULL DEFAULT 1 CHECK (failures_per_record BETWEEN 1 AND 10),
    created_at          timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz NOT NULL,
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '10 minutes')
);

-- Retention -------------------------------------------------------------------------------------------
-- Drops daily events partitions whose whole day ended more than keep_days ago (UTC). Returns the number
-- dropped. The default partition is never dropped.
CREATE FUNCTION drop_expired_event_partitions(keep_days integer) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    cutoff  date := (now() AT TIME ZONE 'UTC')::date - keep_days;
    part    record;
    dropped integer := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('ensure_event_partitions'));
    FOR part IN
        SELECT c.relname
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'events'::regclass AND c.relname ~ '^events_[0-9]{8}$'
    LOOP
        IF to_date(substring(part.relname FROM 8), 'YYYYMMDD') < cutoff THEN
            EXECUTE format('DROP TABLE %I', part.relname);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END
$$;
