package io.github.gladwynl.streamflow.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Read side of simulation runs: producer accounting, sink counts and the latest reconciliation. */
@Repository
public class RunQueries {

    /**
     * @param eventTypes event types the run was restricted to, or null for every type
     * @param processed distinct events of the run in the ledger (first deliveries)
     * @param duplicatesSuppressed redelivered copies suppressed by the ledger
     * @param deadLetters dead letters attributed to the run (all statuses)
     * @param reconciliationResult latest stored result, or null if never reconciled
     * @param reconciliationJson the latest stored report, or null
     */
    public record RunView(
            UUID runId,
            long seed,
            long requestedEvents,
            int ratePerSecond,
            int users,
            List<String> eventTypes,
            String status,
            String statusDetail,
            Instant startedAt,
            Instant finishedAt,
            long attempted,
            long acked,
            long failedDefinite,
            long failedAmbiguous,
            long processed,
            long duplicatesSuppressed,
            long deadLetters,
            String reconciliationResult,
            Instant reconciledAt,
            String reconciliationJson) {}

    private static final String SELECT = """
            SELECT r.run_id, r.seed, r.requested_events, r.rate_per_second, r.users, r.event_types, r.status, r.status_detail,
                   r.started_at, r.finished_at, r.attempted, r.acked, r.failed_definite, r.failed_ambiguous,
                   coalesce((SELECT sum(s.processed) FROM run_stats s WHERE s.run_id = r.run_id), 0) AS processed,
                   coalesce((SELECT sum(s.duplicates_suppressed) FROM run_stats s WHERE s.run_id = r.run_id), 0)
                       AS duplicates,
                   (SELECT count(*) FROM dead_letter_events d WHERE d.run_id = r.run_id) AS dead_letters,
                   c.result, c.checked_at, c.details::text AS details
            FROM simulation_runs r
            LEFT JOIN LATERAL (SELECT result, checked_at, details FROM run_reconciliations rc
                               WHERE rc.run_id = r.run_id ORDER BY checked_at DESC LIMIT 1) c ON true
            """;

    private final JdbcClient jdbc;

    public RunQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Newest first; {@code beforeStartedAt} pages backwards. */
    public List<RunView> list(Instant beforeStartedAt, int limit) {
        return jdbc.sql(SELECT
                        + " WHERE (CAST(:before AS timestamptz) IS NULL OR r.started_at < CAST(:before AS timestamptz))"
                        + " ORDER BY r.started_at DESC LIMIT :limit")
                .param("before", beforeStartedAt == null ? null : Timestamp.from(beforeStartedAt))
                .param("limit", Math.min(limit, DashboardQueries.MAX_PAGE))
                .query(RunQueries::map)
                .list();
    }

    public Optional<RunView> find(UUID runId) {
        return jdbc.sql(SELECT + " WHERE r.run_id = :runId")
                .param("runId", runId)
                .query(RunQueries::map)
                .optional();
    }

    private static List<String> eventTypes(java.sql.Array array) throws SQLException {
        return array == null ? null : List.of((String[]) array.getArray());
    }

    private static RunView map(ResultSet rs, int rowNum) throws SQLException {
        OffsetDateTime finished = rs.getObject("finished_at", OffsetDateTime.class);
        OffsetDateTime reconciled = rs.getObject("checked_at", OffsetDateTime.class);
        return new RunView(
                rs.getObject("run_id", UUID.class),
                rs.getLong("seed"),
                rs.getLong("requested_events"),
                rs.getInt("rate_per_second"),
                rs.getInt("users"),
                eventTypes(rs.getArray("event_types")),
                rs.getString("status"),
                rs.getString("status_detail"),
                rs.getObject("started_at", OffsetDateTime.class).toInstant(),
                finished == null ? null : finished.toInstant(),
                rs.getLong("attempted"),
                rs.getLong("acked"),
                rs.getLong("failed_definite"),
                rs.getLong("failed_ambiguous"),
                rs.getLong("processed"),
                rs.getLong("duplicates"),
                rs.getLong("dead_letters"),
                rs.getString("result"),
                reconciled == null ? null : reconciled.toInstant(),
                rs.getString("details"));
    }
}
