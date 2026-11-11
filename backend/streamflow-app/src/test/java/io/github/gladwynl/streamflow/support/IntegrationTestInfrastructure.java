package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.StreamflowApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
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
 * migrated schema and provisioned topics exactly as Docker Compose would produce them. Tests that move events use
 * their own {@link Pipeline} (topics and consumer group), provisioned by the same migrate role.
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

    private static final DataSource DATA_SOURCE;

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
        runMigrateRole(Map.of()).close();
        DATA_SOURCE = DataSourceBuilder.create()
                .url(POSTGRES.getJdbcUrl())
                .username(POSTGRES.getUsername())
                .password(POSTGRES.getPassword())
                .build();
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
        Map<String, Object> properties = new HashMap<>();
        extraProperties.forEach(properties::put);
        return runRole("migrate", toArguments(properties));
    }

    /** Creates the pipeline's topics and initializes its consumer group, using the real migrate role. */
    public static Pipeline provision(Pipeline pipeline) {
        runRole("migrate", pipeline.arguments()).close();
        return pipeline;
    }

    /** Starts a worker on the pipeline. Close the context to stop it gracefully. */
    public static ConfigurableApplicationContext startWorker(Pipeline pipeline, String... extraArguments) {
        List<String> args = new ArrayList<>(pipeline.arguments());
        args.add("--server.port=0");
        args.add("--streamflow.worker.heartbeat-interval=1s");
        args.addAll(List.of(extraArguments));
        return runRole("worker", args);
    }

    /** Runs the generate role on the pipeline; it returns once the run has been published and reconciled. */
    public static ConfigurableApplicationContext runGenerate(Pipeline pipeline, String... extraArguments) {
        List<String> args = new ArrayList<>(pipeline.arguments());
        args.addAll(List.of(extraArguments));
        return runRole("generate", args);
    }

    /**
     * The command line of a worker in a separate JVM, started from the application jar built by {@code package}
     * (integration tests run after it). Used by the crash tests, which kill that JVM.
     */
    public static List<String> workerProcessCommand(Pipeline pipeline, String... extraArguments) {
        Path jar = Path.of("target", "streamflow-app.jar").toAbsolutePath();
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException("Application jar not found at " + jar + "; run mvnw verify");
        }
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        List<String> command = new ArrayList<>(List.of(java.toString(), "-Xmx256m", "-jar", jar.toString()));
        command.add("--spring.profiles.active=worker");
        command.addAll(connectionArguments());
        command.addAll(pipeline.arguments());
        command.add("--server.port=0");
        command.addAll(List.of(extraArguments));
        return command;
    }

    public static Admin newAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    public static DataSource dataSource() {
        return DATA_SOURCE;
    }

    public static JdbcClient jdbc() {
        return JdbcClient.create(DATA_SOURCE);
    }

    public static Duration adminTimeout() {
        return Duration.ofSeconds(10);
    }

    private static ConfigurableApplicationContext runRole(String role, List<String> roleArguments) {
        List<String> args = new ArrayList<>(connectionArguments());
        args.addAll(roleArguments);
        // Command-line arguments outrank application.yaml; builder "properties" would only be defaults.
        return new SpringApplicationBuilder(StreamflowApplication.class)
                .profiles(role)
                .run(args.toArray(String[]::new));
    }

    private static List<String> connectionArguments() {
        return toArguments(Map.of(
                "spring.datasource.url", POSTGRES.getJdbcUrl(),
                "spring.datasource.username", POSTGRES.getUsername(),
                "spring.datasource.password", POSTGRES.getPassword(),
                "spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers()));
    }

    private static List<String> toArguments(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toList();
    }
}
