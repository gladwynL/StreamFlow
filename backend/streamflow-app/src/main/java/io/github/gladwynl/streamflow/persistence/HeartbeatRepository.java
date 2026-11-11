package io.github.gladwynl.streamflow.persistence;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Worker heartbeats. Every timestamp is taken from the database clock ({@code now()}), so liveness never depends on
 * clock agreement between containers.
 */
@Repository
public class HeartbeatRepository {

    private static final int MAX_LISTED_WORKERS = 100;

    private final JdbcClient jdbc;

    public HeartbeatRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void recordHeartbeat(UUID workerId, String host) {
        recordHeartbeat(workerId, host, WorkerCounters.NONE);
    }

    /** Upserts the heartbeat with the worker's current process-lifetime counters and partition assignment. */
    public void recordHeartbeat(UUID workerId, String host, WorkerCounters c) {
        jdbc.sql("""
                        INSERT INTO worker_heartbeats (worker_id, host, started_at, last_seen_at, processed, duplicates,
                            failures_bad_record, failures_transient, failures_infra, dead_lettered,
                            dead_letter_failures, rebalances, consumer_errors, in_outage, assigned_partitions)
                        VALUES (:workerId, :host, now(), now(), :processed, :duplicates, :bad, :transient, :infra,
                            :deadLettered, :deadLetterFailures, :rebalances, :consumerErrors, :inOutage,
                            CAST(:partitions AS integer[]))
                        ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = now(), processed = EXCLUDED.processed,
                            duplicates = EXCLUDED.duplicates, failures_bad_record = EXCLUDED.failures_bad_record,
                            failures_transient = EXCLUDED.failures_transient,
                            failures_infra = EXCLUDED.failures_infra, dead_lettered = EXCLUDED.dead_lettered,
                            dead_letter_failures = EXCLUDED.dead_letter_failures, rebalances = EXCLUDED.rebalances,
                            consumer_errors = EXCLUDED.consumer_errors, in_outage = EXCLUDED.in_outage,
                            assigned_partitions = EXCLUDED.assigned_partitions
                        """)
                .param("workerId", workerId)
                .param("host", host)
                .param("processed", c.processed())
                .param("duplicates", c.duplicates())
                .param("bad", c.failuresBadRecord())
                .param("transient", c.failuresTransient())
                .param("infra", c.failuresInfrastructure())
                .param("deadLettered", c.deadLettered())
                .param("deadLetterFailures", c.deadLetterFailures())
                .param("rebalances", c.rebalances())
                .param("consumerErrors", c.consumerErrors())
                .param("inOutage", c.inOutage())
                .param(
                        "partitions",
                        "{"
                                + String.join(
                                        ",",
                                        c.assignedPartitions().stream()
                                                .map(String::valueOf)
                                                .toList())
                                + "}")
                .update();
    }

    /** Workers seen within {@code window}, newest first, each flagged live if seen within {@code liveThreshold}. */
    public List<WorkerHeartbeat> findSeenWithin(Duration window, Duration liveThreshold) {
        return jdbc.sql("""
                        SELECT worker_id, host, started_at, last_seen_at,
                               last_seen_at >= now() - make_interval(secs => :liveSeconds) AS live,
                               processed, duplicates, failures_bad_record, failures_transient, failures_infra,
                               dead_lettered, dead_letter_failures, rebalances, consumer_errors, in_outage,
                               assigned_partitions
                        FROM worker_heartbeats
                        WHERE last_seen_at >= now() - make_interval(secs => :windowSeconds)
                        ORDER BY last_seen_at DESC
                        LIMIT :limit
                        """)
                .param("liveSeconds", seconds(liveThreshold))
                .param("windowSeconds", seconds(window))
                .param("limit", MAX_LISTED_WORKERS)
                .query((rs, rowNum) -> new WorkerHeartbeat(
                        rs.getObject("worker_id", UUID.class),
                        rs.getString("host"),
                        rs.getObject("started_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("last_seen_at", OffsetDateTime.class).toInstant(),
                        rs.getBoolean("live"),
                        new WorkerCounters(
                                rs.getLong("processed"),
                                rs.getLong("duplicates"),
                                rs.getLong("failures_bad_record"),
                                rs.getLong("failures_transient"),
                                rs.getLong("failures_infra"),
                                rs.getLong("dead_lettered"),
                                rs.getLong("dead_letter_failures"),
                                rs.getLong("rebalances"),
                                rs.getLong("consumer_errors"),
                                rs.getBoolean("in_outage"),
                                partitions(rs.getArray("assigned_partitions")))))
                .list();
    }

    private static List<Integer> partitions(java.sql.Array array) throws java.sql.SQLException {
        if (array == null) {
            return List.of();
        }
        Integer[] values = (Integer[]) array.getArray();
        return java.util.Arrays.stream(values).sorted().toList();
    }

    /** Deletes heartbeats last seen longer ago than {@code age}; returns the number deleted. */
    public int deleteSeenBefore(Duration age) {
        return jdbc.sql("DELETE FROM worker_heartbeats WHERE last_seen_at < now() - make_interval(secs => :seconds)")
                .param("seconds", seconds(age))
                .update();
    }

    public void delete(UUID workerId) {
        jdbc.sql("DELETE FROM worker_heartbeats WHERE worker_id = :workerId")
                .param("workerId", workerId)
                .update();
    }

    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
