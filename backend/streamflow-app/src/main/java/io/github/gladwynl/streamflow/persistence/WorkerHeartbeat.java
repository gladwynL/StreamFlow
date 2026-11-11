package io.github.gladwynl.streamflow.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * The latest heartbeat of one worker process. Timestamps come from the database clock.
 *
 * @param live whether the heartbeat is newer than the liveness threshold used by the query
 * @param counters the worker's process-lifetime counters as of this heartbeat
 */
public record WorkerHeartbeat(
        UUID workerId, String host, Instant startedAt, Instant lastSeenAt, boolean live, WorkerCounters counters) {}
