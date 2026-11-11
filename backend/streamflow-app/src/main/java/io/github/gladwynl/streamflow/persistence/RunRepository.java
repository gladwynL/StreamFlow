package io.github.gladwynl.streamflow.persistence;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Producer-side accounting of simulation runs. */
@Repository
public class RunRepository {

    public enum RunStatus {
        RUNNING,
        COMPLETED,
        INCOMPLETE
    }

    public enum FailureOutcome {
        /** The record never reached the broker. */
        DEFINITE,
        /** The broker may or may not have stored the record. */
        AMBIGUOUS
    }

    public record RunRecord(
            UUID runId,
            long seed,
            int contentVersion,
            long requestedEvents,
            String topic,
            RunStatus status,
            String statusDetail,
            Instant startedAt,
            long attempted,
            long acked,
            long failedDefinite,
            long failedAmbiguous) {}

    private final JdbcClient jdbc;

    public RunRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void create(
            UUID runId,
            long seed,
            int contentVersion,
            long requestedEvents,
            int ratePerSecond,
            int users,
            String topic) {
        create(runId, seed, contentVersion, requestedEvents, ratePerSecond, users, topic, null);
    }

    /** @param eventTypes the event types the run is restricted to, or null for every type */
    public void create(
            UUID runId,
            long seed,
            int contentVersion,
            long requestedEvents,
            int ratePerSecond,
            int users,
            String topic,
            java.util.List<String> eventTypes) {
        jdbc.sql("""
                        INSERT INTO simulation_runs (run_id, seed, content_version, requested_events, rate_per_second,
                                                     users, topic, status, event_types)
                        VALUES (:runId, :seed, :contentVersion, :requestedEvents, :rate, :users, :topic, 'RUNNING',
                                CAST(:eventTypes AS text[]))
                        """)
                .param("runId", runId)
                .param("seed", seed)
                .param("contentVersion", contentVersion)
                .param("requestedEvents", requestedEvents)
                .param("rate", ratePerSecond)
                .param("users", users)
                .param("topic", topic)
                .param("eventTypes", eventTypes == null ? null : "{" + String.join(",", eventTypes) + "}")
                .update();
    }

    public void recordFailure(UUID runId, long sequence, UUID eventId, FailureOutcome outcome, String error) {
        jdbc.sql("""
                        INSERT INTO publish_failures (run_id, sequence, event_id, outcome, error)
                        VALUES (:runId, :sequence, :eventId, :outcome, :error)
                        ON CONFLICT (run_id, sequence) DO NOTHING
                        """)
                .param("runId", runId)
                .param("sequence", sequence)
                .param("eventId", eventId)
                .param("outcome", outcome.name())
                .param("error", error.length() > 500 ? error.substring(0, 500) : error)
                .update();
    }

    public void finish(
            UUID runId,
            RunStatus status,
            String detail,
            long attempted,
            long acked,
            long failedDefinite,
            long failedAmbiguous) {
        jdbc.sql("""
                        UPDATE simulation_runs
                        SET status = :status, status_detail = :detail, finished_at = now(), attempted = :attempted,
                            acked = :acked, failed_definite = :definite, failed_ambiguous = :ambiguous
                        WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .param("status", status.name())
                .param("detail", detail)
                .param("attempted", attempted)
                .param("acked", acked)
                .param("definite", failedDefinite)
                .param("ambiguous", failedAmbiguous)
                .update();
    }

    public Optional<RunRecord> find(UUID runId) {
        return jdbc.sql("""
                        SELECT run_id, seed, content_version, requested_events, topic, status, status_detail,
                               started_at, attempted, acked, failed_definite, failed_ambiguous
                        FROM simulation_runs WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> new RunRecord(
                        rs.getObject("run_id", UUID.class),
                        rs.getLong("seed"),
                        rs.getInt("content_version"),
                        rs.getLong("requested_events"),
                        rs.getString("topic"),
                        RunStatus.valueOf(rs.getString("status")),
                        rs.getString("status_detail"),
                        rs.getObject("started_at", OffsetDateTime.class).toInstant(),
                        rs.getLong("attempted"),
                        rs.getLong("acked"),
                        rs.getLong("failed_definite"),
                        rs.getLong("failed_ambiguous")))
                .optional();
    }
}
