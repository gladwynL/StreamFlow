package io.github.gladwynl.streamflow.api.deadletters;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.deadletter.ReplayService;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration(proxyBeanMethods = false)
@Profile("api")
class ReplayConfiguration {

    /** Replays through the shared event producer (acks=all, idempotent). */
    @Bean
    ReplayService replayService(
            DeadLetterRepository deadLetters,
            Producer<String, byte[]> eventProducer,
            StreamflowProperties properties,
            CrashInjector crashInjector) {
        return new ReplayService(
                deadLetters, eventProducer, properties.retention().replayWindow(), crashInjector);
    }
}
