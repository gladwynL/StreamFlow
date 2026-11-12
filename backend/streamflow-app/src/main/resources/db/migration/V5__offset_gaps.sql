-- Offset gaps (Phase 6): a consumer group's position on a partition that Kafka no longer holds, because retention
-- deleted records the group had not read. With auto.offset.reset=none the worker pauses the partition and records
-- the gap here instead of skipping. Rows are evidence: they are never updated by recovery and never deleted by
-- retention. Whether a gap is still open is derived from the group's committed offset (docs/runbook.md).
CREATE TABLE offset_gaps (
    id               bigserial   PRIMARY KEY,
    consumer_group   text        NOT NULL,
    topic            text        NOT NULL,
    kafka_partition  integer     NOT NULL,
    lost_position    bigint,                 -- the position that is gone; NULL if the group had no committed offset
    log_start_offset bigint,                 -- first offset Kafka still held when the gap was detected
    events_lost      bigint,                 -- log_start_offset - lost_position, when both are known
    detected_at      timestamptz NOT NULL DEFAULT now(),
    last_seen_at     timestamptz NOT NULL DEFAULT now(),
    occurrences      integer     NOT NULL DEFAULT 1,
    UNIQUE NULLS NOT DISTINCT (consumer_group, topic, kafka_partition, lost_position)
);
