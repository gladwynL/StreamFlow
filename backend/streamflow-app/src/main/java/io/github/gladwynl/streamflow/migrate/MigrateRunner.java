package io.github.gladwynl.streamflow.migrate;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.kafka.KafkaAdminException;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.kafka.TopicProvisioner;
import io.github.gladwynl.streamflow.kafka.TopicProvisioner.ProvisioningResult;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Provisions Kafka topics once Flyway has migrated the database (Spring Boot runs Flyway before application
 * runners). Kafka may still be starting, so admin timeouts are retried until the ready timeout; specification
 * conflicts fail immediately.
 */
@Component
@Profile("migrate")
class MigrateRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MigrateRunner.class);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(2);

    private final TopicProvisioner provisioner;
    private final TopicCatalog catalog;
    private final Duration kafkaReadyTimeout;

    MigrateRunner(TopicProvisioner provisioner, TopicCatalog catalog, StreamflowProperties properties) {
        this.provisioner = provisioner;
        this.catalog = catalog;
        this.kafkaReadyTimeout = properties.migrate().kafkaReadyTimeout();
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        Instant deadline = Instant.now().plus(kafkaReadyTimeout);
        while (true) {
            try {
                ProvisioningResult result = provisioner.provision(catalog.topics());
                log.info(
                        "Kafka topics provisioned: created={}, reconfigured={}, unchanged={}",
                        result.created(),
                        result.reconfigured(),
                        result.unchanged());
                return;
            } catch (KafkaAdminException e) {
                if (Instant.now().plus(RETRY_DELAY).isAfter(deadline)) {
                    throw new IllegalStateException("Kafka was not ready within " + kafkaReadyTimeout, e);
                }
                log.warn("Kafka not ready, retrying in {}: {}", RETRY_DELAY, e.getMessage());
                Thread.sleep(RETRY_DELAY);
            }
        }
    }
}
