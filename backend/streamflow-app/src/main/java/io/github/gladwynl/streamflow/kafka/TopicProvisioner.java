package io.github.gladwynl.streamflow.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Makes the cluster match a set of topic specifications. Safe to run repeatedly.
 *
 * <ul>
 *   <li>Missing topics are created with their partition count, replication factor and managed configs.
 *   <li>Managed configs that drifted are set back to their specified values.
 *   <li>A partition count or replication factor that differs is reported as an error and never changed: adding
 *       partitions would remap message keys and break per-key ordering.
 * </ul>
 */
@Component
public class TopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(TopicProvisioner.class);

    private final Admin admin;
    private final TopicInspector inspector;

    public TopicProvisioner(Admin admin, TopicInspector inspector) {
        this.admin = admin;
        this.inspector = inspector;
    }

    public ProvisioningResult provision(Collection<TopicSpec> specs) {
        Map<String, TopicState> existing =
                inspector.describe(specs.stream().map(TopicSpec::name).toList());

        List<String> created = new ArrayList<>();
        List<String> reconfigured = new ArrayList<>();
        List<String> unchanged = new ArrayList<>();
        for (TopicSpec spec : specs) {
            TopicState state = existing.get(spec.name());
            if (state == null && create(spec)) {
                created.add(spec.name());
                continue;
            }
            if (state == null) {
                // Another process created it between our describe and create; verify what it created.
                state = inspector.describe(List.of(spec.name())).get(spec.name());
            }
            verifyShape(spec, state);
            List<ConfigDrift> drift = spec.configDrift(state);
            if (drift.isEmpty()) {
                unchanged.add(spec.name());
            } else {
                alterConfigs(spec.name(), drift);
                reconfigured.add(spec.name());
            }
        }
        return new ProvisioningResult(created, reconfigured, unchanged);
    }

    private boolean create(TopicSpec spec) {
        NewTopic topic = new NewTopic(spec.name(), spec.partitions(), spec.replicationFactor()).configs(spec.configs());
        try {
            inspector.await(admin.createTopics(List.of(topic)).all(), "create topic " + spec.name());
            log.info(
                    "Created topic {} with {} partitions and replication factor {}",
                    spec.name(),
                    spec.partitions(),
                    spec.replicationFactor());
            return true;
        } catch (KafkaAdminException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return false;
            }
            throw e;
        }
    }

    private static void verifyShape(TopicSpec spec, TopicState state) {
        if (state.partitions() != spec.partitions()) {
            throw new TopicProvisioningException(("Topic %s has %d partitions but %d are specified. StreamFlow never"
                            + " changes the partition count of an existing topic because that remaps message keys;"
                            + " recreate the topic deliberately if the change is intended.")
                    .formatted(spec.name(), state.partitions(), spec.partitions()));
        }
        if (state.replicationFactor() != spec.replicationFactor()) {
            throw new TopicProvisioningException("Topic %s has replication factor %d but %d is specified."
                    .formatted(spec.name(), state.replicationFactor(), spec.replicationFactor()));
        }
    }

    private void alterConfigs(String topic, List<ConfigDrift> drift) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        List<AlterConfigOp> ops = drift.stream()
                .map(d -> new AlterConfigOp(new ConfigEntry(d.key(), d.expected()), AlterConfigOp.OpType.SET))
                .toList();
        inspector.await(admin.incrementalAlterConfigs(Map.of(resource, ops)).all(), "alter configs of " + topic);
        drift.forEach(d -> log.info("Set {} on topic {} from {} to {}", d.key(), topic, d.actual(), d.expected()));
    }

    public record ProvisioningResult(List<String> created, List<String> reconfigured, List<String> unchanged) {

        public ProvisioningResult {
            created = List.copyOf(created);
            reconfigured = List.copyOf(reconfigured);
            unchanged = List.copyOf(unchanged);
        }
    }
}
