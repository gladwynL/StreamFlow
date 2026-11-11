package io.github.gladwynl.streamflow.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Application settings under the {@code streamflow} prefix. */
@ConfigurationProperties("streamflow")
public record StreamflowProperties(
        @DefaultValue Kafka kafka,
        @DefaultValue Worker worker,
        @DefaultValue Migrate migrate,
        @DefaultValue Status status,
        @DefaultValue Generate generate,
        @DefaultValue Retention retention,
        @DefaultValue CrashTest crashTest) {

    /**
     * @param adminTimeout upper bound for each Kafka admin request (cluster description, topic description, creation)
     * @param topics the topics StreamFlow owns; see {@code application.yaml}
     */
    public record Kafka(
            @DefaultValue("5s") Duration adminTimeout,
            @DefaultValue Topics topics) {}

    public record Topics(TopicSettings events, TopicSettings deadLetter) {}

    /**
     * @param name optional override of the contract topic name (used by tests to provision isolated topics)
     * @param configs topic-level Kafka configs that the migrate role owns and keeps in sync
     */
    public record TopicSettings(String name, int partitions, short replicationFactor, Map<String, String> configs) {}

    /**
     * @param heartbeatInterval how often each worker records a heartbeat
     * @param heartbeatRetention heartbeats older than this are deleted when a worker starts
     * @param consumerGroup Kafka consumer group of the event processors
     * @param concurrency consumer threads per worker process; workers × concurrency beyond the partition count idle
     * @param maxPollRecords upper bound on records per poll, and so on records redelivered after a crash
     */
    public record Worker(
            @DefaultValue("5s") Duration heartbeatInterval,
            @DefaultValue("24h") Duration heartbeatRetention,
            @DefaultValue("streamflow-processor") String consumerGroup,
            @DefaultValue("3") int concurrency,
            @DefaultValue("500") int maxPollRecords) {

        /** A worker counts as live if its last heartbeat is newer than three heartbeat intervals. */
        public Duration liveThreshold() {
            return heartbeatInterval.multipliedBy(3);
        }
    }

    /** @param kafkaReadyTimeout how long the migrate role waits for Kafka to answer before failing */
    public record Migrate(@DefaultValue("60s") Duration kafkaReadyTimeout) {}

    /** @param workerWindow how far back the status endpoint lists workers, live or not */
    public record Status(@DefaultValue("10m") Duration workerWindow) {}

    /**
     * One bounded simulation run of the {@code generate} role.
     *
     * @param drainTimeout how long to wait for the workers to process the run before reconciling anyway
     * @param maxPublishFailures the run stops early, as INCOMPLETE, once failures exceed this
     */
    public record Generate(
            @DefaultValue("42") long seed,
            @DefaultValue("10000") long events,
            @DefaultValue("1000") int ratePerSecond,
            @DefaultValue("50000") int users,
            @DefaultValue("120s") Duration drainTimeout,
            @DefaultValue("10000") long maxPublishFailures) {}

    /**
     * Retention of the deduplication ledger and the windows in which an event can come back (ADR-0008).
     *
     * @param ledger how long ledger rows are kept; must cover every redelivery and replay window plus the margin
     * @param replayWindow dead letters older than this cannot be replayed (Phase 3)
     */
    public record Retention(
            @DefaultValue("8d") Duration ledger,
            @DefaultValue("7d") Duration replayWindow,
            @DefaultValue("1d") Duration margin) {}

    /**
     * Crash-recovery test hook; see {@code CrashInjector}. Disabled when {@code point} is unset.
     *
     * @param afterRecords halt when this many records have reached {@code point}
     */
    public record CrashTest(
            CrashPoint point, @DefaultValue("0") long afterRecords) {

        public enum CrashPoint {
            BEFORE_DB_COMMIT,
            AFTER_DB_COMMIT
        }
    }
}
