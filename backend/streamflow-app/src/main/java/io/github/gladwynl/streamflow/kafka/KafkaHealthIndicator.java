package io.github.gladwynl.streamflow.kafka;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Reports UP when the Kafka cluster answers a cluster description within the admin timeout. */
@Component("kafka")
@Profile({"api", "worker"})
class KafkaHealthIndicator implements HealthIndicator {

    private final TopicInspector inspector;

    KafkaHealthIndicator(TopicInspector inspector) {
        this.inspector = inspector;
    }

    @Override
    public Health health() {
        try {
            TopicInspector.ClusterInfo cluster = inspector.describeCluster();
            return Health.up()
                    .withDetail("clusterId", cluster.clusterId())
                    .withDetail("brokers", cluster.brokerCount())
                    .build();
        } catch (KafkaAdminException e) {
            return Health.down().withDetail("error", e.getMessage()).build();
        }
    }
}
