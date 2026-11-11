package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.kafka.ConsumerLagReader;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.persistence.ReconciliationQueries;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import java.time.Clock;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publication and reconciliation of simulation runs, shared by the roles that publish events: {@code generate}
 * (one run from the command line) and {@code api} (runs started from the dashboard, and dead-letter replay).
 */
@Configuration(proxyBeanMethods = false)
@Profile({"api", "generate"})
class SimulationConfiguration {

    /** acks=all, idempotent; used for simulation runs and dead-letter replay. */
    @Bean(destroyMethod = "close")
    Producer<String, byte[]> eventProducer(KafkaProperties kafka) {
        return SimulationProducers.create(String.join(",", kafka.getBootstrapServers()), "streamflow-producer");
    }

    @Bean
    SimulationService simulationService(
            Producer<String, byte[]> eventProducer,
            TopicCatalog topics,
            RunRepository runs,
            EventCodec codec,
            EventIds ids,
            StreamflowProperties properties) {
        return new SimulationService(
                eventProducer,
                topics.events().name(),
                runs,
                codec,
                ids,
                properties.generate().maxPublishFailures(),
                Clock.systemUTC());
    }

    @Bean
    ReconciliationService reconciliationService(
            RunRepository runs,
            ReconciliationQueries queries,
            ConsumerLagReader lagReader,
            TopicCatalog topics,
            JsonMapper json,
            StreamflowProperties properties) {
        return new ReconciliationService(
                runs,
                queries,
                lagReader,
                new ReconciliationService.Topology(
                        properties.worker().consumerGroup(),
                        topics.events(),
                        properties.deadLetters().ingestorGroup(),
                        topics.deadLetter()),
                properties.retention().events(),
                json,
                Clock.systemUTC());
    }
}
