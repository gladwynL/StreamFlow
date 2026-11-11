package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.kafka.TopicSpec;
import java.util.List;
import java.util.Map;

/**
 * An isolated event pipeline for one integration test: its own topics and consumer groups, so tests never consume
 * each other's events even while Spring caches worker contexts of other test classes.
 */
public record Pipeline(String eventsTopic, String deadLetterTopic, String consumerGroup, String ingestorGroup) {

    public static final int PARTITIONS = 6;

    public static Pipeline named(String name) {
        return new Pipeline("it." + name + ".events", "it." + name + ".dlt", "it-" + name, "it-" + name + "-dlt");
    }

    public TopicSpec eventsSpec() {
        return new TopicSpec(eventsTopic, PARTITIONS, (short) 1, Map.of());
    }

    public TopicSpec deadLetterSpec() {
        return new TopicSpec(deadLetterTopic, PARTITIONS, (short) 1, Map.of());
    }

    /** Command-line arguments that point a role at this pipeline. */
    public List<String> arguments() {
        return List.of(
                "--streamflow.kafka.topics.events.name=" + eventsTopic,
                "--streamflow.kafka.topics.dead-letter.name=" + deadLetterTopic,
                "--streamflow.worker.consumer-group=" + consumerGroup,
                "--streamflow.dead-letters.ingestor-group=" + ingestorGroup);
    }
}
