package io.github.gladwynl.streamflow.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventTopics;
import io.github.gladwynl.streamflow.kafka.TopicProvisioningException;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Runs the real migrate role against real PostgreSQL and Kafka. Tests that change topics use their own topic names,
 * so the shared contract topics stay untouched for the other integration tests.
 */
class MigrateRoleIT {

    private static Admin admin;
    private static JdbcClient jdbc;

    @BeforeAll
    static void connect() {
        admin = IntegrationTestInfrastructure.newAdmin();
        jdbc = JdbcClient.create(DataSourceBuilder.create()
                .url(IntegrationTestInfrastructure.jdbcUrl())
                .username(IntegrationTestInfrastructure.jdbcUsername())
                .password(IntegrationTestInfrastructure.jdbcPassword())
                .build());
    }

    @AfterAll
    static void disconnect() {
        admin.close();
    }

    @Test
    void appliesTheSchemaMigration() {
        List<String> applied = jdbc.sql(
                        "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank")
                .query(String.class)
                .list();

        assertThat(applied).containsExactly("1");
        assertThat(jdbc.sql("SELECT to_regclass('worker_heartbeats') IS NOT NULL")
                        .query(Boolean.class)
                        .single())
                .isTrue();
    }

    @Test
    void createsContractTopicsWithSpecifiedShapeAndConfigs() throws Exception {
        assertShape(EventTopics.EVENTS, 6, 1);
        assertShape(EventTopics.EVENTS_DEAD_LETTER, 6, 1);

        assertThat(configs(EventTopics.EVENTS))
                .containsEntry("cleanup.policy", "delete")
                .containsEntry("retention.ms", "86400000")
                .containsEntry("retention.bytes", "1073741824")
                .containsEntry("segment.bytes", "134217728");
        assertThat(configs(EventTopics.EVENTS_DEAD_LETTER))
                .containsEntry("retention.ms", "604800000")
                .containsEntry("retention.bytes", "268435456");
    }

    @Test
    void isIdempotentAndRepairsConfigDrift() throws Exception {
        Map<String, Object> isolatedTopics = topicNames("it.migrate.drift.events", "it.migrate.drift.dlt");
        IntegrationTestInfrastructure.runMigrateRole(isolatedTopics).close();
        assertShape("it.migrate.drift.events", 6, 1);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "it.migrate.drift.events");
        AlterConfigOp shortRetention =
                new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOp.OpType.SET);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(shortRetention)))
                .all()
                .get(10, TimeUnit.SECONDS);
        // KRaft applies config changes to the broker's metadata cache asynchronously.
        awaitConfig("it.migrate.drift.events", "retention.ms", "1000");

        IntegrationTestInfrastructure.runMigrateRole(isolatedTopics).close();
        awaitConfig("it.migrate.drift.events", "retention.ms", "86400000");

        IntegrationTestInfrastructure.runMigrateRole(isolatedTopics).close();
        assertShape("it.migrate.drift.events", 6, 1);
        assertThat(configs("it.migrate.drift.events")).containsEntry("retention.ms", "86400000");
    }

    @Test
    void refusesToChangeThePartitionCountOfAnExistingTopic() throws Exception {
        admin.createTopics(List.of(new NewTopic("it.migrate.narrow", 3, (short) 1)))
                .all()
                .get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertShape("it.migrate.narrow", 3, 1));

        assertThatThrownBy(() -> IntegrationTestInfrastructure.runMigrateRole(
                                topicNames("it.migrate.narrow", "it.migrate.narrow.dlt"))
                        .close())
                .isInstanceOf(TopicProvisioningException.class)
                .hasMessageContaining("it.migrate.narrow has 3 partitions but 6 are specified");

        assertShape("it.migrate.narrow", 3, 1);
    }

    private static void awaitConfig(String topic, String key, String value) {
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(configs(topic)).containsEntry(key, value));
    }

    private static Map<String, Object> topicNames(String events, String deadLetter) {
        return Map.of(
                "streamflow.kafka.topics.events.name", events,
                "streamflow.kafka.topics.dead-letter.name", deadLetter);
    }

    private static void assertShape(String topic, int partitions, int replicationFactor) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
        assertThat(description.partitions()).hasSize(partitions);
        assertThat(description.partitions())
                .allSatisfy(p -> assertThat(p.replicas()).hasSize(replicationFactor));
    }

    private static Map<String, String> configs(String topic) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(10, TimeUnit.SECONDS)
                .get(resource);
        return config.entries().stream()
                .filter(entry -> entry.value() != null)
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    }
}
