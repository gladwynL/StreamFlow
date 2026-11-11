package io.github.gladwynl.streamflow.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Read-only queries behind the dashboard. Every query is bounded by a time window, a {@code LIMIT} or a keyset
 * cursor, and uses an index: {@code events (ingested_at DESC)}, {@code events (event_type, ingested_at DESC)},
 * {@code events (run_id)}, the primary keys of the minute aggregates, and {@code dead_letter_events
 * (dead_lettered_at DESC)} / {@code (event_id)}.
 */
@Repository
public class DashboardQueries {

    public static final int MAX_PAGE = 100;

    public record EventRow(
            UUID eventId,
            String eventType,
            Instant occurredAt,
            Instant kafkaTimestamp,
            Instant ingestedAt,
            int kafkaPartition,
            long kafkaOffset,
            UUID runId,
            long runSeq,
            String userId,
            String sessionId,
            Long amountCents,
            String currency,
            long latencyMillis,
            boolean replayed) {}

    public record EventDetail(EventRow event, String payloadJson) {}

    /** One minute of activity. Latency counts are the histogram buckets of that minute. */
    public record MinutePoint(
            Instant minute, long processed, long amountCents, long deadLettered, long[] latencyBuckets) {}

    public record TypeCount(String eventType, long events, long amountCents) {}

    public record DeadLetterRow(
            long id,
            UUID eventId,
            String failureCategory,
            String errorMessage,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Instant deadLetteredAt,
            String status) {}

    private static final String EVENT_COLUMNS = """
            e.event_id, e.event_type, e.occurred_at, e.kafka_ts, e.ingested_at, e.kafka_partition, e.kafka_offset,
            e.run_id, e.run_seq, e.user_id, e.session_id, e.amount_cents, e.currency,
            greatest(0, floor(extract(epoch FROM e.ingested_at - e.kafka_ts) * 1000))::bigint AS latency_ms,
            EXISTS (SELECT 1 FROM dead_letter_events d
                    WHERE d.event_id = e.event_id AND d.status = 'REPLAYED') AS replayed
            """;

    private final JdbcClient jdbc;

    public DashboardQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Events processed (first deliveries) within the last {@code window}, by ingestion time. */
    public long processedWithin(Duration window) {
        return jdbc.sql("SELECT count(*) FROM events WHERE ingested_at > now() - make_interval(secs => :s)")
                .param("s", seconds(window))
                .query(Long.class)
                .single();
    }

    public long deadLetteredWithin(Duration window) {
        return jdbc.sql(
                        "SELECT count(*) FROM dead_letter_events WHERE dead_lettered_at > now() - make_interval(secs => :s)")
                .param("s", seconds(window))
                .query(Long.class)
                .single();
    }

    /** Latency histogram summed over the minute buckets that overlap the last {@code window}. */
    public long[] latencyHistogram(Duration window) {
        long[] counts = new long[LatencyBuckets.BUCKETS];
        jdbc.sql("""
                        SELECT bucket_index, sum(event_count) AS n FROM latency_stats_minute
                        WHERE bucket_start >= date_trunc('minute', now() - make_interval(secs => :s))
                        GROUP BY bucket_index
                        """)
                .param("s", seconds(window))
                .query((rs, rowNum) -> {
                    counts[rs.getInt("bucket_index")] = rs.getLong("n");
                    return null;
                })
                .list();
        return counts;
    }

    /** Per-minute processing, dead letters and latency over the last {@code range}, oldest first, gaps omitted. */
    public List<MinutePoint> minuteSeries(Duration range) {
        List<MinutePoint> points = new ArrayList<>();
        jdbc.sql("""
                        WITH since AS (SELECT date_trunc('minute', now() - make_interval(secs => :s)) AS t),
                        processed AS (
                            SELECT bucket_start AS minute, sum(event_count) AS n, sum(amount_cents) AS amount
                            FROM event_stats_minute, since WHERE bucket_start >= since.t GROUP BY bucket_start),
                        dead AS (
                            SELECT date_trunc('minute', dead_lettered_at) AS minute, count(*) AS n
                            FROM dead_letter_events, since WHERE dead_lettered_at >= since.t GROUP BY 1),
                        latency AS (
                            SELECT bucket_start AS minute, array_agg(bucket_index ORDER BY bucket_index) AS idx,
                                   array_agg(n ORDER BY bucket_index) AS counts
                            FROM (SELECT bucket_start, bucket_index, sum(event_count) AS n
                                  FROM latency_stats_minute, since WHERE bucket_start >= since.t
                                  GROUP BY bucket_start, bucket_index) b
                            GROUP BY bucket_start)
                        SELECT m.minute, coalesce(p.n, 0) AS processed, coalesce(p.amount, 0) AS amount,
                               coalesce(d.n, 0) AS dead, l.idx, l.counts
                        FROM (SELECT minute FROM processed UNION SELECT minute FROM dead) m
                        LEFT JOIN processed p USING (minute)
                        LEFT JOIN dead d USING (minute)
                        LEFT JOIN latency l USING (minute)
                        ORDER BY m.minute
                        """)
                .param("s", seconds(range))
                .query((rs, rowNum) -> {
                    long[] buckets = new long[LatencyBuckets.BUCKETS];
                    java.sql.Array idx = rs.getArray("idx");
                    java.sql.Array counts = rs.getArray("counts");
                    if (idx != null && counts != null) {
                        Object[] i = (Object[]) idx.getArray();
                        Object[] c = (Object[]) counts.getArray();
                        for (int k = 0; k < i.length; k++) {
                            buckets[((Number) i[k]).intValue()] = ((Number) c[k]).longValue();
                        }
                    }
                    points.add(new MinutePoint(
                            rs.getObject("minute", OffsetDateTime.class).toInstant(),
                            rs.getLong("processed"),
                            rs.getLong("amount"),
                            rs.getLong("dead"),
                            buckets));
                    return null;
                })
                .list();
        return points;
    }

    public List<TypeCount> distribution(Duration range) {
        return jdbc.sql("""
                        SELECT event_type, sum(event_count) AS n, sum(amount_cents) AS amount FROM event_stats_minute
                        WHERE bucket_start >= date_trunc('minute', now() - make_interval(secs => :s))
                        GROUP BY event_type ORDER BY n DESC
                        """)
                .param("s", seconds(range))
                .query((rs, rowNum) -> new TypeCount(rs.getString("event_type"), rs.getLong("n"), rs.getLong("amount")))
                .list();
    }

    public List<TypeCount> runDistribution(UUID runId) {
        return jdbc.sql("""
                        SELECT event_type, count(*) AS n, coalesce(sum(amount_cents), 0) AS amount FROM events
                        WHERE run_id = :runId GROUP BY event_type ORDER BY n DESC
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> new TypeCount(rs.getString("event_type"), rs.getLong("n"), rs.getLong("amount")))
                .list();
    }

    /** Newest processed events, optionally of one type. */
    public List<EventRow> recentEvents(String eventType, int limit) {
        return jdbc.sql("SELECT " + EVENT_COLUMNS + " FROM events e"
                        + " WHERE (CAST(:type AS text) IS NULL OR e.event_type = :type)"
                        + " AND e.ingested_at > now() - interval '1 day'"
                        + " ORDER BY e.ingested_at DESC LIMIT :limit")
                .param("type", eventType)
                .param("limit", Math.min(limit, MAX_PAGE))
                .query(DashboardQueries::eventRow)
                .list();
    }

    /**
     * A page of processed events, newest first. The cursor is the (ingestedAt, eventId) of the last row of the
     * previous page.
     */
    public List<EventRow> events(
            String eventType, UUID runId, Instant beforeIngestedAt, UUID beforeEventId, int limit) {
        return jdbc.sql("SELECT " + EVENT_COLUMNS + " FROM events e"
                        + " WHERE (CAST(:type AS text) IS NULL OR e.event_type = :type)"
                        + " AND (CAST(:runId AS uuid) IS NULL OR e.run_id = :runId)"
                        + " AND (CAST(:beforeTs AS timestamptz) IS NULL"
                        + "      OR (e.ingested_at, e.event_id) < (CAST(:beforeTs AS timestamptz), CAST(:beforeId AS uuid)))"
                        + " ORDER BY e.ingested_at DESC, e.event_id DESC LIMIT :limit")
                .param("type", eventType)
                .param("runId", runId)
                .param("beforeTs", beforeIngestedAt == null ? null : Timestamp.from(beforeIngestedAt))
                .param("beforeId", beforeEventId)
                // One more than a page, so callers can tell whether another page follows.
                .param("limit", Math.min(limit, MAX_PAGE + 1))
                .query(DashboardQueries::eventRow)
                .list();
    }

    public Optional<EventDetail> event(UUID eventId) {
        return jdbc.sql("SELECT " + EVENT_COLUMNS + ", e.payload::text AS payload FROM events e WHERE e.event_id = :id")
                .param("id", eventId)
                .query((rs, rowNum) -> new EventDetail(eventRow(rs, rowNum), rs.getString("payload")))
                .optional();
    }

    public List<DeadLetterRow> recentDeadLetters(int limit) {
        return jdbc.sql("""
                        SELECT id, event_id, failure_category, error_message, source_topic, source_partition,
                               source_offset, dead_lettered_at, status
                        FROM dead_letter_events WHERE dead_lettered_at > now() - interval '1 day'
                        ORDER BY dead_lettered_at DESC LIMIT :limit
                        """)
                .param("limit", Math.min(limit, MAX_PAGE))
                .query((rs, rowNum) -> new DeadLetterRow(
                        rs.getLong("id"),
                        rs.getObject("event_id", UUID.class),
                        rs.getString("failure_category"),
                        rs.getString("error_message"),
                        rs.getString("source_topic"),
                        rs.getInt("source_partition"),
                        rs.getLong("source_offset"),
                        rs.getObject("dead_lettered_at", OffsetDateTime.class).toInstant(),
                        rs.getString("status")))
                .list();
    }

    private static EventRow eventRow(ResultSet rs, int rowNum) throws SQLException {
        long amount = rs.getLong("amount_cents");
        Long amountValue = rs.wasNull() ? null : amount;
        return new EventRow(
                rs.getObject("event_id", UUID.class),
                rs.getString("event_type"),
                rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                rs.getObject("kafka_ts", OffsetDateTime.class).toInstant(),
                rs.getObject("ingested_at", OffsetDateTime.class).toInstant(),
                rs.getInt("kafka_partition"),
                rs.getLong("kafka_offset"),
                rs.getObject("run_id", UUID.class),
                rs.getLong("run_seq"),
                rs.getString("user_id"),
                rs.getString("session_id"),
                amountValue,
                rs.getString("currency"),
                rs.getLong("latency_ms"),
                rs.getBoolean("replayed"));
    }

    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
