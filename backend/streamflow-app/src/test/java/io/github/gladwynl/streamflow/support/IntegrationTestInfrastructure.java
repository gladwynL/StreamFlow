package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.StreamflowApplication;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One Kafka and one PostgreSQL container shared by every integration test in the JVM, started on first use and
 * removed by Testcontainers when the JVM exits. Sharing keeps memory use and run time low on a laptop.
 *
 * <p>On startup the real migrate role runs once against the containers, so every integration test starts from a
 * migrated schema and provisioned topics exactly as Docker Compose would produce them.
 */
public final class IntegrationTestInfrastructure {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse(TestImages.POSTGRES))
            .withDatabaseName("streamflow")
            .withUsername("streamflow")
            .withPassword("streamflow-it");

    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse(TestImages.KAFKA))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
        runMigrateRole(Map.of()).close();
    }

    private IntegrationTestInfrastructure() {}

    public static void registerPostgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    public static void registerKafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    public static void registerAll(DynamicPropertyRegistry registry) {
        registerPostgres(registry);
        registerKafka(registry);
    }

    /**
     * Runs the migrate role to completion, as {@code docker compose} does, with optional extra properties.
     *
     * @return the started context; callers close it
     */
    public static ConfigurableApplicationContext runMigrateRole(Map<String, Object> extraProperties) {
        Map<String, Object> properties = new HashMap<>(Map.of(
                "spring.datasource.url", POSTGRES.getJdbcUrl(),
                "spring.datasource.username", POSTGRES.getUsername(),
                "spring.datasource.password", POSTGRES.getPassword(),
                "spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers()));
        properties.putAll(extraProperties);
        // Command-line arguments outrank application.yaml; builder "properties" would only be defaults.
        String[] args = properties.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new);
        return new SpringApplicationBuilder(StreamflowApplication.class)
                .profiles("migrate")
                .run(args);
    }

    public static Admin newAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    public static String jdbcUsername() {
        return POSTGRES.getUsername();
    }

    public static String jdbcPassword() {
        return POSTGRES.getPassword();
    }
}
