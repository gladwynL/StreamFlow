package io.github.gladwynl.streamflow.kafka;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

/** Reads cluster and topic state through the Kafka admin API, bounding every call by the admin timeout. */
@Component
public class TopicInspector {

    private final Admin admin;
    private final Duration timeout;

    public TopicInspector(Admin admin, StreamflowProperties properties) {
        this.admin = admin;
        this.timeout = properties.kafka().adminTimeout();
    }

    public ClusterInfo describeCluster() {
        DescribeClusterResult result = admin.describeCluster();
        String clusterId = await(result.clusterId(), "describe cluster");
        int brokerCount = await(result.nodes(), "describe cluster nodes").size();
        return new ClusterInfo(clusterId, brokerCount);
    }

    /** Returns the state of each named topic that exists. Missing topics are absent from the result. */
    public Map<String, TopicState> describe(Collection<String> topicNames) {
        Map<String, TopicDescription> descriptions = new HashMap<>();
        admin.describeTopics(topicNames).topicNameValues().forEach((name, future) -> {
            TopicDescription description = awaitUnlessMissing(future, "describe topic " + name);
            if (description != null) {
                descriptions.put(name, description);
            }
        });
        if (descriptions.isEmpty()) {
            return Map.of();
        }

        List<ConfigResource> resources = descriptions.keySet().stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .toList();
        Map<ConfigResource, Config> configs =
                await(admin.describeConfigs(resources).all(), "describe topic configs");

        Map<String, TopicState> states = new HashMap<>();
        descriptions.forEach((name, description) -> {
            Map<String, String> values = new HashMap<>();
            Config config = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name));
            if (config != null) {
                config.entries().forEach(entry -> {
                    if (entry.value() != null) {
                        values.put(entry.name(), entry.value());
                    }
                });
            }
            int replicationFactor =
                    description.partitions().getFirst().replicas().size();
            states.put(name, new TopicState(name, description.partitions().size(), replicationFactor, values));
        });
        return Map.copyOf(states);
    }

    <T> T await(KafkaFuture<T> future, String operation) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("Interrupted during Kafka " + operation, e);
        } catch (ExecutionException e) {
            throw new KafkaAdminException(
                    "Kafka " + operation + " failed: " + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new KafkaAdminException("Kafka did not answer " + operation + " within " + timeout, e);
        }
    }

    private <T> T awaitUnlessMissing(KafkaFuture<T> future, String operation) {
        try {
            return await(future, operation);
        } catch (KafkaAdminException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return null;
            }
            throw e;
        }
    }

    public record ClusterInfo(String clusterId, int brokerCount) {}
}
