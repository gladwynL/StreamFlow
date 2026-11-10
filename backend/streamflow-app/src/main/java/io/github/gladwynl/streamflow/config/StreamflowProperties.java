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
        @DefaultValue Status status) {

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
     */
    public record Worker(
            @DefaultValue("5s") Duration heartbeatInterval,
            @DefaultValue("24h") Duration heartbeatRetention) {

        /** A worker counts as live if its last heartbeat is newer than three heartbeat intervals. */
        public Duration liveThreshold() {
            return heartbeatInterval.multipliedBy(3);
        }
    }

    /** @param kafkaReadyTimeout how long the migrate role waits for Kafka to answer before failing */
    public record Migrate(@DefaultValue("60s") Duration kafkaReadyTimeout) {}

    /** @param workerWindow how far back the status endpoint lists workers, live or not */
    public record Status(@DefaultValue("10m") Duration workerWindow) {}
}
