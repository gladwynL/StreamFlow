package io.github.gladwynl.streamflow.api.status;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.api.status.SystemStatus.DatabaseStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.KafkaStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.Overall;
import io.github.gladwynl.streamflow.api.status.SystemStatus.TopicStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.WorkerStatus;
import io.github.gladwynl.streamflow.kafka.ConfigDrift;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SystemStatusTest {

    private static final Instant NOW = Instant.parse("2026-11-10T12:00:00Z");
    private static final TopicStatus HEALTHY_TOPIC = new TopicStatus("events", true, 6, 6, 1, List.of());
    private static final KafkaStatus KAFKA_OK = new KafkaStatus(true, "cluster", 1, List.of(HEALTHY_TOPIC), null);
    private static final DatabaseStatus DATABASE_OK = new DatabaseStatus(true, "18.6", "1", null);
    private static final WorkerStatus LIVE_WORKER = new WorkerStatus(UUID.randomUUID(), "host", NOW, NOW, true);
    private static final WorkerStatus STALE_WORKER = new WorkerStatus(UUID.randomUUID(), "host", NOW, NOW, false);

    @Test
    void okWhenEveryCheckPasses() {
        SystemStatus status = SystemStatus.evaluate(NOW, KAFKA_OK, DATABASE_OK, List.of(LIVE_WORKER), "15s", Map.of());

        assertThat(status.status()).isEqualTo(Overall.OK);
        assertThat(status.issues()).isEmpty();
        assertThat(status.checkedAt()).isEqualTo(NOW);
    }

    @Test
    void degradedWhenKafkaIsUnreachable() {
        SystemStatus status = SystemStatus.evaluate(
                NOW, KafkaStatus.unreachable("timed out"), DATABASE_OK, List.of(LIVE_WORKER), "15s", Map.of());

        assertThat(status.status()).isEqualTo(Overall.DEGRADED);
        assertThat(status.issues()).containsExactly("Kafka is unreachable: timed out");
    }

    @Test
    void reportsMissingTopicPartitionMismatchAndConfigDrift() {
        KafkaStatus kafka = new KafkaStatus(
                true,
                "cluster",
                1,
                List.of(
                        new TopicStatus("missing", false, null, 6, null, List.of()),
                        new TopicStatus("narrow", true, 3, 6, 1, List.of()),
                        new TopicStatus(
                                "drifted",
                                true,
                                6,
                                6,
                                1,
                                List.of(new ConfigDrift("retention.ms", "86400000", "1000")))),
                null);

        SystemStatus status = SystemStatus.evaluate(NOW, kafka, DATABASE_OK, List.of(LIVE_WORKER), "15s", Map.of());

        assertThat(status.status()).isEqualTo(Overall.DEGRADED);
        assertThat(status.issues())
                .containsExactly(
                        "Topic missing is missing",
                        "Topic narrow has 3 partitions; expected 6",
                        "Topic drifted config retention.ms is 1000; expected 86400000");
    }

    @Test
    void degradedWhenNoWorkerIsLive() {
        SystemStatus status = SystemStatus.evaluate(NOW, KAFKA_OK, DATABASE_OK, List.of(STALE_WORKER), "15s", Map.of());

        assertThat(status.issues()).containsExactly("No worker has sent a heartbeat within 15s");
    }

    @Test
    void unreachableDatabaseIsReportedOnceWithoutAWorkerIssue() {
        SystemStatus status = SystemStatus.evaluate(
                NOW, KAFKA_OK, DatabaseStatus.unreachable("connection refused"), List.of(), "15s", Map.of());

        assertThat(status.issues()).containsExactly("PostgreSQL is unreachable: connection refused");
    }

    @Test
    void degradedWhenSchemaIsNotMigrated() {
        DatabaseStatus unmigrated = new DatabaseStatus(true, "18.6", null, null);

        SystemStatus status = SystemStatus.evaluate(NOW, KAFKA_OK, unmigrated, List.of(LIVE_WORKER), "15s", Map.of());

        assertThat(status.issues()).containsExactly("The database schema has not been migrated");
    }

    @Test
    void warnsWhenAPartitionWithUnreadRecordsNearsItsRetentionSize() {
        long limit = 1_000;
        List<String> warnings = SystemStatus.retentionWarnings(
                "events", Map.of(0, 5L, 1, 0L, 2, 9L), Map.of(0, 850L, 1, 990L, 2, 500L), limit);
        // Partition 1 is full but fully read (no risk); partition 2 lags but is half full.
        assertThat(warnings).hasSize(1);
        assertThat(warnings.getFirst()).contains("events-0").contains("85 %").contains("5 unread records");
        assertThat(SystemStatus.retentionWarnings("events", Map.of(0, 5L), Map.of(0, 850L), null))
                .isEmpty();

        SystemStatus status = SystemStatus.evaluate(
                NOW, KAFKA_OK, DATABASE_OK, List.of(LIVE_WORKER), "15s", Map.of(), List.of(), warnings);
        assertThat(status.status()).isEqualTo(SystemStatus.Overall.DEGRADED);
        assertThat(status.issues()).containsExactlyElementsOf(warnings);
    }
}
