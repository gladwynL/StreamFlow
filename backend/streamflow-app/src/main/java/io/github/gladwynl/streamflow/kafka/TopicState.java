package io.github.gladwynl.streamflow.kafka;

import java.util.Map;

/** The observed state of an existing Kafka topic. */
public record TopicState(String name, int partitions, int replicationFactor, Map<String, String> configs) {

    public TopicState {
        configs = Map.copyOf(configs);
    }
}
