package io.github.gladwynl.streamflow.api.status;

import io.github.gladwynl.streamflow.kafka.ConfigDrift;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Response of {@code GET /api/v1/system/status}: real observations of Kafka, PostgreSQL and the workers, plus the
 * issues that make the system degraded. Every value comes from a live check made while answering the request.
 */
public record SystemStatus(
        Overall status,
        Instant checkedAt,
        List<String> issues,
        KafkaStatus kafka,
        DatabaseStatus database,
        List<WorkerStatus> workers) {

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
            String workerLiveThreshold) {
        List<String> issues = new ArrayList<>();
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
        Overall overall = issues.isEmpty() ? Overall.OK : Overall.DEGRADED;
        return new SystemStatus(overall, checkedAt, List.copyOf(issues), kafka, database, List.copyOf(workers));
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
