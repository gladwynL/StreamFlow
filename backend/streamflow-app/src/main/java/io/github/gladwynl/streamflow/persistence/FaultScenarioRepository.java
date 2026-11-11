package io.github.gladwynl.streamflow.persistence;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Predefined, time-limited fault-injection scenarios (at most ten minutes each; enforced by the schema). */
@Repository
public class FaultScenarioRepository {

    public enum Kind {
        /** Every event of the type fails as a bad record (simulates a faulty release). */
        REJECT_EVENT_TYPE,
        /** Every event of the type fails transiently on its first {@code failuresPerRecord} delivery attempts. */
        TRANSIENT_FAILURES
    }

    public record Scenario(long id, Kind kind, String eventType, int failuresPerRecord, Instant expiresAt) {}

    private final JdbcClient jdbc;

    public FaultScenarioRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long activate(Kind kind, String eventType, int failuresPerRecord, Duration duration) {
        return jdbc.sql("""
                        INSERT INTO fault_scenarios (kind, event_type, failures_per_record, expires_at)
                        VALUES (:kind, :eventType, :failures, now() + make_interval(secs => :seconds))
                        RETURNING id
                        """)
                .param("kind", kind.name())
                .param("eventType", eventType)
                .param("failures", failuresPerRecord)
                .param("seconds", duration.toMillis() / 1000.0)
                .query(Long.class)
                .single();
    }

    /** Ends every active scenario now. */
    public int deactivateAll() {
        return jdbc.sql("UPDATE fault_scenarios SET expires_at = created_at + interval '1 microsecond'"
                        + " WHERE expires_at > now()")
                .update();
    }

    public List<Scenario> active() {
        return jdbc.sql("""
                        SELECT id, kind, event_type, failures_per_record, expires_at FROM fault_scenarios
                        WHERE expires_at > now() ORDER BY id
                        """)
                .query((rs, rowNum) -> new Scenario(
                        rs.getLong("id"),
                        Kind.valueOf(rs.getString("kind")),
                        rs.getString("event_type"),
                        rs.getInt("failures_per_record"),
                        rs.getObject("expires_at", OffsetDateTime.class).toInstant()))
                .list();
    }
}
