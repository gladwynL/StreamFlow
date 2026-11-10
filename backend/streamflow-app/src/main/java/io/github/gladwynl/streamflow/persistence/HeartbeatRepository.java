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
        jdbc.sql("""
                        INSERT INTO worker_heartbeats (worker_id, host, started_at, last_seen_at)
                        VALUES (:workerId, :host, now(), now())
                        ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = now()
                        """).param("workerId", workerId).param("host", host).update();
    }

    /** Workers seen within {@code window}, newest first, each flagged live if seen within {@code liveThreshold}. */
    public List<WorkerHeartbeat> findSeenWithin(Duration window, Duration liveThreshold) {
        return jdbc.sql("""
                        SELECT worker_id, host, started_at, last_seen_at,
                               last_seen_at >= now() - make_interval(secs => :liveSeconds) AS live
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
                        rs.getBoolean("live")))
                .list();
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
