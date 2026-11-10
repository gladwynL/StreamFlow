-- Liveness of worker processes. One row per worker process; rows of stopped workers are
-- deleted on graceful shutdown, and stale rows are purged when a worker starts.
CREATE TABLE worker_heartbeats (
    worker_id    uuid        PRIMARY KEY,
    host         text        NOT NULL CHECK (length(host) BETWEEN 1 AND 255),
    started_at   timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL
);

CREATE INDEX worker_heartbeats_last_seen_idx ON worker_heartbeats (last_seen_at DESC);

COMMENT ON TABLE worker_heartbeats IS 'Worker liveness; timestamps use the database clock.';
