package io.github.gladwynl.streamflow.persistence;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Evidence of consumer positions that Kafka retention deleted before they were read (table {@code offset_gaps}). */
@Repository
public class OffsetGapRepository {

    /**
     * @param lostPosition the position that is gone; null if the group had no committed offset
     * @param logStartOffset first offset Kafka still held at detection; null if it could not be read
     * @param eventsLost log start minus lost position, when both are known
     */
    public record OffsetGap(
            long id,
            String consumerGroup,
            String topic,
            int partition,
            Long lostPosition,
            Long logStartOffset,
            Long eventsLost,
            Instant detectedAt,
            Instant lastSeenAt,
            int occurrences) {}

    private static final int MAX_LISTED = 100;

    private final JdbcClient jdbc;

    public OffsetGapRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records a gap, or counts another detection of the same gap (same group, partition and lost position, for
     * example after a rebalance). Returns true if the gap is new.
     */
    public boolean record(String group, String topic, int partition, Long lostPosition, Long logStartOffset) {
        Long eventsLost = lostPosition != null && logStartOffset != null ? logStartOffset - lostPosition : null;
        int occurrences = jdbc.sql("""
                        INSERT INTO offset_gaps (consumer_group, topic, kafka_partition, lost_position, log_start_offset,
                                                 events_lost)
                        VALUES (:group, :topic, :partition, :lost, :logStart, :eventsLost)
                        ON CONFLICT (consumer_group, topic, kafka_partition, lost_position) DO UPDATE
                        SET last_seen_at = now(), occurrences = offset_gaps.occurrences + 1
                        RETURNING occurrences
                        """)
                .param("group", group)
                .param("topic", topic)
                .param("partition", partition)
                .param("lost", lostPosition)
                .param("logStart", logStartOffset)
                .param("eventsLost", eventsLost)
                .query(Integer.class)
                .single();
        return occurrences == 1;
    }

    /** The most recently detected gaps, newest first. */
    public List<OffsetGap> recent() {
        return jdbc.sql("""
                        SELECT id, consumer_group, topic, kafka_partition, lost_position, log_start_offset, events_lost,
                               detected_at, last_seen_at, occurrences
                        FROM offset_gaps ORDER BY detected_at DESC LIMIT :limit
                        """)
                .param("limit", MAX_LISTED)
                .query((rs, rowNum) -> new OffsetGap(
                        rs.getLong("id"),
                        rs.getString("consumer_group"),
                        rs.getString("topic"),
                        rs.getInt("kafka_partition"),
                        (Long) rs.getObject("lost_position"),
                        (Long) rs.getObject("log_start_offset"),
                        (Long) rs.getObject("events_lost"),
                        rs.getObject("detected_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("last_seen_at", OffsetDateTime.class).toInstant(),
                        rs.getInt("occurrences")))
                .list();
    }
}
