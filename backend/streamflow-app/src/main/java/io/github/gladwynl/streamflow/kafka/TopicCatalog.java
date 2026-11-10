package io.github.gladwynl.streamflow.kafka;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.config.StreamflowProperties.TopicSettings;
import io.github.gladwynl.streamflow.events.EventTopics;
import java.util.List;
import org.springframework.stereotype.Component;

/** The topics StreamFlow owns, built from the contract names and the configured operational settings. */
@Component
public class TopicCatalog {

    private final List<TopicSpec> topics;

    public TopicCatalog(StreamflowProperties properties) {
        StreamflowProperties.Topics settings = properties.kafka().topics();
        this.topics = List.of(
                spec(settings.events(), EventTopics.EVENTS, "events"),
                spec(settings.deadLetter(), EventTopics.EVENTS_DEAD_LETTER, "dead-letter"));
    }

    public List<TopicSpec> topics() {
        return topics;
    }

    private static TopicSpec spec(TopicSettings settings, String contractName, String key) {
        if (settings == null) {
            throw new IllegalStateException("Missing configuration: streamflow.kafka.topics." + key);
        }
        String name = settings.name() == null || settings.name().isBlank() ? contractName : settings.name();
        return new TopicSpec(name, settings.partitions(), settings.replicationFactor(), settings.configs());
    }
}
