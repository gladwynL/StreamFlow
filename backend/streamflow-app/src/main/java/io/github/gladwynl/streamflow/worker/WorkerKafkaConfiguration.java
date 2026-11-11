package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Consumer settings of the event processors (ADR-0005, docs/verification/spring-kafka-behavior.md):
 *
 * <ul>
 *   <li>record listener, one database transaction per record;
 *   <li>auto-commit disabled, {@code AckMode.BATCH}: offsets are committed after every record of a poll has been
 *       processed, so each offset is committed after its record's database commit;
 *   <li>{@code auto.offset.reset=none}: a missing or out-of-range position fails loudly instead of skipping data;
 *   <li>Phase 2 failure handling: every failure is retried in place without limit (0.5 s doubling to 10 s), so an
 *       unprocessable record stalls its partition and is never skipped. Phase 3 adds classification and the
 *       dead-letter topic.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@Profile("worker")
class WorkerKafkaConfiguration {

    static final String CONTAINER_FACTORY = "eventListenerContainerFactory";

    @Bean(CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, byte[]> eventListenerContainerFactory(
            KafkaProperties kafka, StreamflowProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafka.getBootstrapServers()));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.worker().maxPollRecords());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.setConcurrency(properties.worker().concurrency());
        factory.getContainerProperties().setAckMode(AckMode.BATCH);
        factory.getContainerProperties().setClientId("streamflow-worker");
        factory.setCommonErrorHandler(new DefaultErrorHandler(retryForever()));
        return factory;
    }

    private static ExponentialBackOff retryForever() {
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxInterval(10_000);
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }
}
