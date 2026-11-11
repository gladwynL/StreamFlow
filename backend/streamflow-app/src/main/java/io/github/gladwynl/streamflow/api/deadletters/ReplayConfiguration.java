package io.github.gladwynl.streamflow.api.deadletters;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.deadletter.ReplayService;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import io.github.gladwynl.streamflow.simulation.SimulationProducers;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration(proxyBeanMethods = false)
@Profile("api")
class ReplayConfiguration {

    /** Same producer guarantees as the generator: acks=all and idempotence. */
    @Bean(destroyMethod = "close")
    Producer<String, byte[]> replayProducer(KafkaProperties kafka) {
        return SimulationProducers.create(String.join(",", kafka.getBootstrapServers()), "streamflow-replay");
    }

    @Bean
    ReplayService replayService(
            DeadLetterRepository deadLetters,
            Producer<String, byte[]> replayProducer,
            StreamflowProperties properties,
            CrashInjector crashInjector) {
        return new ReplayService(
                deadLetters, replayProducer, properties.retention().replayWindow(), crashInjector);
    }
}
