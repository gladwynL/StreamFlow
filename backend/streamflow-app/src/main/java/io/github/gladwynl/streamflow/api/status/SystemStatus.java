package io.github.gladwynl.streamflow.api.status;

import io.github.gladwynl.streamflow.kafka.ConfigDrift;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Response of {@code GET /api/v1/system/status}: real observations of Kafka, PostgreSQL and the workers, plus the
 * issues that make the system degraded. Every value comes from a live check made while answering the request.
 *
 * <p>{@code deadLetters} counts dead letters by status (empty when PostgreSQL is unreachable). It is informational:
 * unresolved dead letters await an operator but do not make the system degraded.
 */
public record SystemStatus(
        Overall status,
        Instant checkedAt,
        List<String> issues,
        KafkaStatus kafka,
        DatabaseStatus database,
        List<WorkerStatus> workers,
        Map<String, Long> deadLetters,
        List<OffsetGapStatus> offsetGaps) {

    /** A partition with unread records is at least this full relative to {@code retention.bytes}. */
    static final double RETENTION_WARNING_FILL = 0.8;

    /**
     * Partitions at risk of an offset gap: they hold unread records (lag) and are at least 80 % of the topic's
     * {@code retention.bytes}, beyond which Kafka deletes the oldest records whether they were read or not.
     */
    static List<String> retentionWarnings(
            String topic, Map<Integer, Long> lagByPartition, Map<Integer, Long> sizes, Long retentionBytes) {
        List<String> warnings = new ArrayList<>();
        if (retentionBytes == null || retentionBytes <= 0) {
            return warnings;
        }
        new java.util.TreeMap<>(sizes).forEach((partition, size) -> {
            long lag = lagByPartition.getOrDefault(partition, 0L);
            double fill = (double) size / retentionBytes;
            if (lag > 0 && fill >= RETENTION_WARNING_FILL) {
                warnings.add(("Partition %s-%d is %.0f %% of its retention size limit with %d unread records: Kafka"
                                + " deletes the oldest records beyond the limit, read or not. Add or speed up"
                                + " workers (docs/runbook.md \"Backlog and disk\").")
                        .formatted(topic, partition, fill * 100, lag));
            }
        });
        return warnings;
    }

    /**
     * A recorded offset gap (table {@code offset_gaps}) and whether it is still open: the group's committed offset is
     * still below the first offset Kafka holds, so the partition stays paused until an operator resets it.
     *
     * @param open null when it could not be determined (Kafka unreachable); treated as open
     * @param committedOffset the group's committed offset on the partition now, or null
     */
    public record OffsetGapStatus(
            String consumerGroup,
            String topic,
            int partition,
            Long lostPosition,
            Long logStartOffset,
            Long eventsLost,
            Instant detectedAt,
            int occurrences,
            Boolean open,
            Long committedOffset) {}

    public enum Overall {
        OK,
        DEGRADED
    }

    /** Combines the individual checks into an overall status and a list of human-readable issues. */
    static SystemStatus evaluate(
            Instant checkedAt,
            KafkaStatus kafka,
            DatabaseStatus database,
            List<WorkerStatus> workers,
            String workerLiveThreshold,
            Map<String, Long> deadLetters) {
        return evaluate(checkedAt, kafka, database, workers, workerLiveThreshold, deadLetters, List.of());
    }

    static SystemStatus evaluate(
            Instant checkedAt,
            KafkaStatus kafka,
            DatabaseStatus database,
            List<WorkerStatus> workers,
            String workerLiveThreshold,
            Map<String, Long> deadLetters,
            List<OffsetGapStatus> offsetGaps) {
        return evaluate(checkedAt, kafka, database, workers, workerLiveThreshold, deadLetters, offsetGaps, List.of());
    }

    static SystemStatus evaluate(
            Instant checkedAt,
            KafkaStatus kafka,
            DatabaseStatus database,
            List<WorkerStatus> workers,
            String workerLiveThreshold,
            Map<String, Long> deadLetters,
            List<OffsetGapStatus> offsetGaps,
            List<String> retentionWarnings) {
        List<String> issues = new ArrayList<>(retentionWarnings);
        if (!kafka.reachable()) {
            issues.add("Kafka is unreachable: " + kafka.error());
        }
        for (TopicStatus topic : kafka.topics()) {
            if (!topic.present()) {
                issues.add("Topic " + topic.name() + " is missing");
                continue;
            }
            if (topic.partitions() != topic.expectedPartitions()) {
                issues.add("Topic %s has %d partitions; expected %d"
                        .formatted(topic.name(), topic.partitions(), topic.expectedPartitions()));
            }
            for (ConfigDrift drift : topic.configDrift()) {
                issues.add("Topic %s config %s is %s; expected %s"
                        .formatted(topic.name(), drift.key(), drift.actual(), drift.expected()));
            }
        }
        if (!database.reachable()) {
            issues.add("PostgreSQL is unreachable: " + database.error());
        } else if (database.schemaVersion() == null) {
            issues.add("The database schema has not been migrated");
        }
        if (database.reachable() && workers.stream().noneMatch(WorkerStatus::live)) {
            issues.add("No worker has sent a heartbeat within " + workerLiveThreshold);
        }
        for (OffsetGapStatus gap : offsetGaps) {
            if (!Boolean.FALSE.equals(gap.open())) {
                issues.add(("Offset gap on %s-%d for group %s: position %s was deleted by retention before it was read"
                                + " (%s events lost; log starts at %s). The partition is paused; nothing was skipped."
                                + " Operator recovery: docs/runbook.md \"Offset gaps\".")
                        .formatted(
                                gap.topic(),
                                gap.partition(),
                                gap.consumerGroup(),
                                gap.lostPosition() == null ? "(none)" : gap.lostPosition(),
                                gap.eventsLost() == null ? "unknown" : gap.eventsLost(),
                                gap.logStartOffset() == null ? "unknown" : gap.logStartOffset()));
            }
        }
        Overall overall = issues.isEmpty() ? Overall.OK : Overall.DEGRADED;
        return new SystemStatus(
                overall,
                checkedAt,
                List.copyOf(issues),
                kafka,
                database,
                List.copyOf(workers),
                Map.copyOf(deadLetters),
                List.copyOf(offsetGaps));
    }

    /**
     * @param clusterId {@code null} when Kafka is unreachable
     * @param brokerCount {@code null} when Kafka is unreachable
     * @param topics empty when Kafka is unreachable
     */
    public record KafkaStatus(
            boolean reachable, String clusterId, Integer brokerCount, List<TopicStatus> topics, String error) {

        static KafkaStatus unreachable(String error) {
            return new KafkaStatus(false, null, null, List.of(), error);
        }
    }

    /**
     * @param partitions {@code null} when the topic is missing
     * @param replicationFactor {@code null} when the topic is missing
     */
    public record TopicStatus(
            String name,
            boolean present,
            Integer partitions,
            int expectedPartitions,
            Integer replicationFactor,
            List<ConfigDrift> configDrift) {}

    /** @param schemaVersion latest applied Flyway migration, or {@code null} if none */
    public record DatabaseStatus(boolean reachable, String serverVersion, String schemaVersion, String error) {

        static DatabaseStatus unreachable(String error) {
            return new DatabaseStatus(false, null, null, error);
        }
    }

    public record WorkerStatus(UUID workerId, String host, Instant startedAt, Instant lastSeenAt, boolean live) {}
}
