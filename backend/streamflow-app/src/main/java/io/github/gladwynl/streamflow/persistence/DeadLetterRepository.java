package io.github.gladwynl.streamflow.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Dead letters and their lifecycle: {@code PENDING → REPLAY_REQUESTED → REPLAYED}, or {@code PENDING → DISCARDED} /
 * {@code EXPIRED}. Every state change is a conditional update, so concurrent callers cannot both win.
 */
@Repository
public class DeadLetterRepository {

    public enum Status {
        PENDING,
        REPLAY_REQUESTED,
        REPLAYED,
        DISCARDED,
        EXPIRED
    }

    /** A dead-letter record as read from the dead-letter topic. */
    public record NewDeadLetter(
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Instant sourceTimestamp,
            int dltPartition,
            long dltOffset,
            UUID eventId,
            UUID runId,
            Long runSeq,
            String recordKey,
            byte[] payload,
            String headersJson,
            String failureCategory,
            String exceptionClass,
            String errorMessage,
            Integer attempts,
            Instant deadLetteredAt) {}

    public record DeadLetter(
            long id,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            UUID eventId,
            UUID runId,
            Long runSeq,
            String recordKey,
            byte[] payload,
            String headersJson,
            String failureCategory,
            String exceptionClass,
            String errorMessage,
            Integer attempts,
            Instant deadLetteredAt,
            Status status,
            Instant statusChangedAt,
            int replayCount,
            Instant replayedAt,
            String discardReason) {}

    private static final String COLUMNS = """
            id, source_topic, source_partition, source_offset, event_id, run_id, run_seq, record_key, payload,
            headers::text AS headers, failure_category, exception_class, error_message, attempts, dead_lettered_at,
            status, status_changed_at, replay_count, replayed_at, discard_reason
            """;

    private final JdbcClient jdbc;

    public DeadLetterRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Stores a dead letter unless one already exists for the same source record. Returns {@code true} if new. */
    public boolean insertIfAbsent(NewDeadLetter d) {
        return jdbc.sql("""
                        INSERT INTO dead_letter_events (source_topic, source_partition, source_offset, source_timestamp,
                            dlt_partition, dlt_offset, event_id, run_id, run_seq, record_key, payload, headers,
                            failure_category, exception_class, error_message, attempts, dead_lettered_at)
                        VALUES (:sourceTopic, :sourcePartition, :sourceOffset, :sourceTimestamp, :dltPartition,
                            :dltOffset, :eventId, :runId, :runSeq, :recordKey, :payload, CAST(:headers AS jsonb),
                            :failureCategory, :exceptionClass, :errorMessage, :attempts, :deadLetteredAt)
                        ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
                        """)
                        .param("sourceTopic", d.sourceTopic())
                        .param("sourcePartition", d.sourcePartition())
                        .param("sourceOffset", d.sourceOffset())
                        .param(
                                "sourceTimestamp",
                                d.sourceTimestamp() == null ? null : Timestamp.from(d.sourceTimestamp()))
                        .param("dltPartition", d.dltPartition())
                        .param("dltOffset", d.dltOffset())
                        .param("eventId", d.eventId())
                        .param("runId", d.runId())
                        .param("runSeq", d.runSeq())
                        .param("recordKey", d.recordKey())
                        .param("payload", d.payload())
                        .param("headers", d.headersJson())
                        .param("failureCategory", d.failureCategory())
                        .param("exceptionClass", d.exceptionClass())
                        .param("errorMessage", d.errorMessage())
                        .param("attempts", d.attempts())
                        .param("deadLetteredAt", Timestamp.from(d.deadLetteredAt()))
                        .update()
                == 1;
    }

    public Optional<DeadLetter> find(long id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM dead_letter_events WHERE id = :id")
                .param("id", id)
                .query(DeadLetterRepository::map)
                .optional();
    }

    /** Newest first; {@code beforeId} pages backwards. {@code status} may be null for every status. */
    public List<DeadLetter> list(Status status, Long beforeId, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM dead_letter_events
                        WHERE (CAST(:status AS text) IS NULL OR status = :status)
                          AND (CAST(:beforeId AS bigint) IS NULL OR id < :beforeId)
                        ORDER BY id DESC LIMIT :limit
                        """)
                .param("status", status == null ? null : status.name())
                .param("beforeId", beforeId)
                .param("limit", limit)
                .query(DeadLetterRepository::map)
                .list();
    }

    public List<DeadLetter> findByEventId(UUID eventId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM dead_letter_events WHERE event_id = :eventId ORDER BY id")
                .param("eventId", eventId)
                .query(DeadLetterRepository::map)
                .list();
    }

    public Map<Status, Long> countsByStatus() {
        Map<Status, Long> counts = new java.util.EnumMap<>(Status.class);
        for (Status status : Status.values()) {
            counts.put(status, 0L);
        }
        jdbc.sql("SELECT status, count(*) AS n FROM dead_letter_events GROUP BY status")
                .query((rs, rowNum) -> Map.entry(Status.valueOf(rs.getString("status")), rs.getLong("n")))
                .list()
                .forEach(e -> counts.put(e.getKey(), e.getValue()));
        return counts;
    }

    /**
     * Claims replayable dead letters for one replay: {@code PENDING}, with an event id, dead-lettered within the
     * window. Rows are locked in id order ({@code FOR UPDATE}), so concurrent claims cannot deadlock; a claim that
     * waited for a lock re-checks the row and skips it if another claim changed its status meanwhile.
     *
     * @return the claimed rows
     */
    public List<DeadLetter> claim(List<Long> ids, UUID claimToken, Duration replayWindow) {
        return jdbc.sql("WITH claimable AS ("
                        + " SELECT id AS claim_id FROM dead_letter_events"
                        + " WHERE id IN (:ids) AND status = 'PENDING' AND event_id IS NOT NULL"
                        + " AND dead_lettered_at >= now() - make_interval(secs => :windowSeconds)"
                        + " ORDER BY id FOR UPDATE)"
                        + " UPDATE dead_letter_events SET status = 'REPLAY_REQUESTED', claim_token = :token,"
                        + " claimed_at = now(), status_changed_at = now(), replay_count = replay_count + 1"
                        + " FROM claimable WHERE id = claimable.claim_id"
                        + " RETURNING " + COLUMNS)
                .param("token", claimToken)
                .param("ids", ids)
                .param("windowSeconds", replayWindow.toMillis() / 1000.0)
                .query(DeadLetterRepository::map)
                .list();
    }

    /** Completes a claim after the replayed record was acknowledged. */
    public boolean markReplayed(long id, UUID claimToken) {
        return jdbc.sql("UPDATE dead_letter_events SET status = 'REPLAYED', replayed_at = now(),"
                                + " status_changed_at = now(), claim_token = NULL, claimed_at = NULL"
                                + " WHERE id = :id AND claim_token = :token")
                        .param("id", id)
                        .param("token", claimToken)
                        .update()
                == 1;
    }

    /** Returns a claim to PENDING after a publication that definitely failed. */
    public boolean release(long id, UUID claimToken) {
        return jdbc.sql("UPDATE dead_letter_events SET status = 'PENDING', status_changed_at = now(),"
                                + " claim_token = NULL, claimed_at = NULL WHERE id = :id AND claim_token = :token")
                        .param("id", id)
                        .param("token", claimToken)
                        .update()
                == 1;
    }

    /**
     * Returns claims older than {@code timeout} to PENDING: their replayer is presumed crashed. Whether or not its
     * publication reached Kafka, a later replay is safe because the ledger suppresses a second delivery.
     */
    public int releaseStaleClaims(Duration timeout) {
        return jdbc.sql("UPDATE dead_letter_events SET status = 'PENDING', status_changed_at = now(),"
                        + " claim_token = NULL, claimed_at = NULL WHERE status = 'REPLAY_REQUESTED'"
                        + " AND claimed_at < now() - make_interval(secs => :seconds)")
                .param("seconds", timeout.toMillis() / 1000.0)
                .update();
    }

    public boolean discard(long id, String reason) {
        return jdbc.sql("UPDATE dead_letter_events SET status = 'DISCARDED', discard_reason = :reason,"
                                + " status_changed_at = now() WHERE id = :id AND status IN ('PENDING', 'EXPIRED')")
                        .param("id", id)
                        .param("reason", reason)
                        .update()
                == 1;
    }

    /** Marks PENDING dead letters older than the replay window as EXPIRED (never deletes them). */
    public int expire(Duration replayWindow) {
        return jdbc.sql("UPDATE dead_letter_events SET status = 'EXPIRED', status_changed_at = now()"
                        + " WHERE status = 'PENDING'"
                        + " AND dead_lettered_at < now() - make_interval(secs => :seconds)")
                .param("seconds", replayWindow.toMillis() / 1000.0)
                .update();
    }

    /** Deletes REPLAYED and DISCARDED dead letters resolved longer ago than {@code age}. */
    public int deleteResolvedBefore(Duration age) {
        return jdbc.sql("DELETE FROM dead_letter_events WHERE status IN ('REPLAYED', 'DISCARDED')"
                        + " AND status_changed_at < now() - make_interval(secs => :seconds)")
                .param("seconds", age.toMillis() / 1000.0)
                .update();
    }

    private static DeadLetter map(ResultSet rs, int rowNum) throws SQLException {
        long runSeq = rs.getLong("run_seq");
        Long runSeqValue = rs.wasNull() ? null : runSeq;
        int attempts = rs.getInt("attempts");
        Integer attemptsValue = rs.wasNull() ? null : attempts;
        OffsetDateTime replayedAt = rs.getObject("replayed_at", OffsetDateTime.class);
        return new DeadLetter(
                rs.getLong("id"),
                rs.getString("source_topic"),
                rs.getInt("source_partition"),
                rs.getLong("source_offset"),
                rs.getObject("event_id", UUID.class),
                rs.getObject("run_id", UUID.class),
                runSeqValue,
                rs.getString("record_key"),
                rs.getBytes("payload"),
                rs.getString("headers"),
                rs.getString("failure_category"),
                rs.getString("exception_class"),
                rs.getString("error_message"),
                attemptsValue,
                rs.getObject("dead_lettered_at", OffsetDateTime.class).toInstant(),
                Status.valueOf(rs.getString("status")),
                rs.getObject("status_changed_at", OffsetDateTime.class).toInstant(),
                rs.getInt("replay_count"),
                replayedAt == null ? null : replayedAt.toInstant(),
                rs.getString("discard_reason"));
    }
}
