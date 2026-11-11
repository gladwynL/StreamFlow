package io.github.gladwynl.streamflow.kafka;

import io.github.gladwynl.streamflow.config.RetentionPolicyCheck;
import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.config.StreamflowProperties.TopicSettings;
import io.github.gladwynl.streamflow.events.EventTopics;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * The topics StreamFlow owns, built from the contract names and the configured operational settings. Construction
 * fails if the configured retention would break the deduplication guarantee, so no role starts with such settings.
 */
@Component
public class TopicCatalog {

    private final TopicSpec events;
    private final TopicSpec deadLetter;

    public TopicCatalog(StreamflowProperties properties) {
        StreamflowProperties.Topics settings = properties.kafka().topics();
        this.events = spec(settings.events(), EventTopics.EVENTS, "events");
        this.deadLetter = spec(settings.deadLetter(), EventTopics.EVENTS_DEAD_LETTER, "dead-letter");
        RetentionPolicyCheck.verify(properties.retention(), timeRetention());
    }

    public List<TopicSpec> topics() {
        return List.of(events, deadLetter);
    }

    public TopicSpec events() {
        return events;
    }

    public TopicSpec deadLetter() {
        return deadLetter;
    }

    private Map<String, Duration> timeRetention() {
        Map<String, Duration> retention = new LinkedHashMap<>();
        for (TopicSpec topic : topics()) {
            String retentionMs = topic.configs().get("retention.ms");
            if (retentionMs != null && !retentionMs.equals("-1")) {
                retention.put(topic.name(), Duration.ofMillis(Long.parseLong(retentionMs)));
            }
        }
        return retention;
    }

    private static TopicSpec spec(TopicSettings settings, String contractName, String key) {
        if (settings == null) {
            throw new IllegalStateException("Missing configuration: streamflow.kafka.topics." + key);
        }
        String name = settings.name() == null || settings.name().isBlank() ? contractName : settings.name();
        return new TopicSpec(name, settings.partitions(), settings.replicationFactor(), settings.configs());
    }
}
