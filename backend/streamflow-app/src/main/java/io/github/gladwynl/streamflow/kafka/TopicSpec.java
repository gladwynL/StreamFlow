package io.github.gladwynl.streamflow.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The desired shape of a Kafka topic that StreamFlow owns.
 *
 * @param configs topic-level configs that must match exactly; configs not listed here are left to broker defaults
 */
public record TopicSpec(String name, int partitions, short replicationFactor, Map<String, String> configs) {

    public TopicSpec {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be blank");
        }
        if (partitions < 1) {
            throw new IllegalArgumentException("Topic " + name + " must have at least one partition");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Topic " + name + " must have a replication factor of at least 1");
        }
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }

    /** Returns the managed configs whose actual value differs from the desired one, ordered by key. */
    public List<ConfigDrift> configDrift(TopicState actual) {
        List<ConfigDrift> drift = new ArrayList<>();
        new TreeMap<>(configs).forEach((key, expected) -> {
            String actualValue = actual.configs().get(key);
            if (!expected.equals(actualValue)) {
                drift.add(new ConfigDrift(key, expected, actualValue));
            }
        });
        return List.copyOf(drift);
    }
}
