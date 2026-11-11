package io.github.gladwynl.streamflow.api.status;

import io.github.gladwynl.streamflow.api.status.SystemStatus.DatabaseStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.KafkaStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.TopicStatus;
import io.github.gladwynl.streamflow.api.status.SystemStatus.WorkerStatus;
import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.kafka.KafkaAdminException;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.kafka.TopicInspector;
import io.github.gladwynl.streamflow.kafka.TopicSpec;
import io.github.gladwynl.streamflow.kafka.TopicState;
import io.github.gladwynl.streamflow.persistence.DatabaseInfoRepository;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@Profile("api")
class SystemStatusService {

    private static final Logger log = LoggerFactory.getLogger(SystemStatusService.class);
    private static final int MAX_ERROR_LENGTH = 200;

    private final TopicInspector inspector;
    private final TopicCatalog catalog;
    private final DatabaseInfoRepository database;
    private final HeartbeatRepository heartbeats;
    private final DeadLetterRepository deadLetters;
    private final Duration workerWindow;
    private final Duration workerLiveThreshold;

    SystemStatusService(
            TopicInspector inspector,
            TopicCatalog catalog,
            DatabaseInfoRepository database,
            HeartbeatRepository heartbeats,
            DeadLetterRepository deadLetters,
            StreamflowProperties properties) {
        this.inspector = inspector;
        this.catalog = catalog;
        this.database = database;
        this.heartbeats = heartbeats;
        this.deadLetters = deadLetters;
        this.workerWindow = properties.status().workerWindow();
        this.workerLiveThreshold = properties.worker().liveThreshold();
    }

    SystemStatus currentStatus() {
        Instant checkedAt = Instant.now();
        KafkaStatus kafka = kafkaStatus();
        DatabaseStatus databaseStatus = databaseStatus();
        List<WorkerStatus> workers = databaseStatus.reachable() ? workers() : List.of();
        Map<String, Long> deadLetterCounts = databaseStatus.reachable() ? deadLetterCounts() : Map.of();
        return SystemStatus.evaluate(
                checkedAt, kafka, databaseStatus, workers, workerLiveThreshold.toSeconds() + "s", deadLetterCounts);
    }

    private Map<String, Long> deadLetterCounts() {
        try {
            Map<String, Long> counts = new LinkedHashMap<>();
            deadLetters.countsByStatus().forEach((status, count) -> counts.put(status.name(), count));
            return counts;
        } catch (DataAccessException e) {
            log.warn("Dead-letter count query failed: {}", e.getMessage());
            return Map.of();
        }
    }

    private KafkaStatus kafkaStatus() {
        try {
            TopicInspector.ClusterInfo cluster = inspector.describeCluster();
            List<TopicSpec> specs = catalog.topics();
            Map<String, TopicState> states =
                    inspector.describe(specs.stream().map(TopicSpec::name).toList());
            List<TopicStatus> topics = specs.stream()
                    .map(spec -> topicStatus(spec, states.get(spec.name())))
                    .toList();
            return new KafkaStatus(true, cluster.clusterId(), cluster.brokerCount(), topics, null);
        } catch (KafkaAdminException e) {
            log.warn("Kafka status check failed: {}", e.getMessage());
            return KafkaStatus.unreachable(truncate(e.getMessage()));
        }
    }

    private static TopicStatus topicStatus(TopicSpec spec, TopicState state) {
        if (state == null) {
            return new TopicStatus(spec.name(), false, null, spec.partitions(), null, List.of());
        }
        return new TopicStatus(
                spec.name(),
                true,
                state.partitions(),
                spec.partitions(),
                state.replicationFactor(),
                spec.configDrift(state));
    }

    private DatabaseStatus databaseStatus() {
        try {
            String schemaVersion = database.schemaVersion().orElse(null);
            return new DatabaseStatus(true, database.serverVersion(), schemaVersion, null);
        } catch (DataAccessException e) {
            log.warn("Database status check failed: {}", e.getMessage());
            return DatabaseStatus.unreachable(truncate(e.getMostSpecificCause().getMessage()));
        }
    }

    private List<WorkerStatus> workers() {
        try {
            return heartbeats.findSeenWithin(workerWindow, workerLiveThreshold).stream()
                    .map(h -> new WorkerStatus(h.workerId(), h.host(), h.startedAt(), h.lastSeenAt(), h.live()))
                    .toList();
        } catch (DataAccessException e) {
            log.warn("Worker heartbeat query failed: {}", e.getMessage());
            return List.of();
        }
    }

    private static String truncate(String message) {
        if (message == null) {
            return "unknown error";
        }
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) + "…" : message;
    }
}
