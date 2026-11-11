package io.github.gladwynl.streamflow.worker;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.config.StreamflowProperties.Retry;
import io.github.gladwynl.streamflow.deadletter.DeadLetterHeaders;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.github.gladwynl.streamflow.processing.FailureClassifier;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer settings of the worker's two listener containers (ADR-0005, ADR-0006, ADR-0007).
 *
 * <p><b>Event processing.</b> Record listener, one database transaction per record, auto-commit disabled,
 * {@code AckMode.BATCH}: offsets are committed after the records' database commits. {@code auto.offset.reset=none}
 * makes a missing or out-of-range position fail loudly instead of skipping data. Failures are classified per
 * attempt:
 *
 * <ul>
 *   <li>bad record: no retry, dead-lettered immediately;
 *   <li>transient: up to {@code transient-max-attempts} attempts with exponential back-off, then dead-lettered;
 *   <li>infrastructure: unlimited exponential back-off capped at {@code infrastructure-max-backoff}; never
 *       dead-lettered, so an outage pauses the partition instead of flooding the dead-letter topic.
 * </ul>
 *
 * Dead-letter publication is synchronous ({@code acks=all}, waits for the broker). If it fails, the recoverer throws,
 * the record is sought back and redelivered, and its offset is not committed. Retry counts live in memory per
 * partition assignment and restart after a worker restart or rebalance.
 *
 * <p><b>Dead-letter ingestion.</b> Same commit model; any failure is retried without limit (the table insert is
 * idempotent and only fails when PostgreSQL is unavailable).
 */
@Configuration(proxyBeanMethods = false)
@Profile("worker")
class WorkerKafkaConfiguration {

    static final String EVENT_CONTAINER_FACTORY = "eventListenerContainerFactory";
    static final String DEAD_LETTER_CONTAINER_FACTORY = "deadLetterListenerContainerFactory";

    private static final Logger log = LoggerFactory.getLogger(WorkerKafkaConfiguration.class);
    private static final Duration DEAD_LETTER_SEND_TIMEOUT = Duration.ofSeconds(15);

    @Bean
    KafkaTemplate<String, byte[]> deadLetterTemplate(KafkaProperties kafka) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafka.getBootstrapServers()));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "streamflow-dead-letters");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    @Bean(EVENT_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, byte[]> eventListenerContainerFactory(
            KafkaProperties kafka,
            StreamflowProperties properties,
            TopicCatalog topics,
            KafkaTemplate<String, byte[]> deadLetterTemplate,
            CrashInjector crashInjector,
            WorkerMetrics metrics) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                containerFactory(kafka, properties.worker().maxPollRecords(), metrics);
        factory.setConcurrency(properties.worker().concurrency());
        factory.getContainerProperties().setClientId("streamflow-worker");
        // Exposes the per-assignment delivery attempt to the listener (used by fault injection).
        factory.getContainerProperties().setDeliveryAttemptHeader(true);

        AtomicReference<DefaultErrorHandler> handlerRef = new AtomicReference<>();
        DeadLetterPublishingRecoverer publisher =
                deadLetterPublisher(deadLetterTemplate, topics.deadLetter().name(), handlerRef);
        ConsumerRecordRecoverer recoverer = (record, failure) -> {
            publisher.accept(record, failure);
            metrics.deadLettered(FailureClassifier.classify(failure));
            crashInjector.afterDeadLetterPublished(record);
        };

        ClassifiedErrorHandler handler = new ClassifiedErrorHandler(recoverer, metrics);
        handler.setBackOffFunction((record, failure) ->
                backOff(FailureClassifier.classify(failure), properties.worker().retry()));
        handler.setResetStateOnExceptionChange(true);
        handler.setRetryListeners(new MetricsRetryListener(metrics));
        handlerRef.set(handler);
        factory.setCommonErrorHandler(handler);
        return factory;
    }

    @Bean(DEAD_LETTER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, byte[]> deadLetterListenerContainerFactory(
            KafkaProperties kafka, StreamflowProperties properties, WorkerMetrics metrics) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                containerFactory(kafka, properties.worker().maxPollRecords(), metrics);
        factory.setConcurrency(1);
        factory.getContainerProperties().setClientId("streamflow-dlt-ingestor");
        Retry retry = properties.worker().retry();
        ClassifiedErrorHandler handler = new ClassifiedErrorHandler(
                (record, failure) -> {
                    throw new IllegalStateException("Dead-letter ingestion never gives up on a record", failure);
                },
                metrics);
        handler.setBackOffFunction((record, failure) -> unlimited(retry));
        handler.setRetryListeners((record, failure, attempt) -> {
            metrics.ingestionFailures.increment();
            log.warn(
                    "Dead-letter ingestion of {}-{}@{} failed (attempt {}), retrying: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    attempt,
                    MetricsRetryListener.rootMessage(failure));
        });
        factory.setCommonErrorHandler(handler);
        return factory;
    }

    private static ConcurrentKafkaListenerContainerFactory<String, byte[]> containerFactory(
            KafkaProperties kafka, int maxPollRecords, WorkerMetrics metrics) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafka.getBootstrapServers()));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.getContainerProperties().setAckMode(AckMode.BATCH);
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    metrics.rebalances.increment();
                }
            }
        });
        return factory;
    }

    private static DeadLetterPublishingRecoverer deadLetterPublisher(
            KafkaTemplate<String, byte[]> template,
            String deadLetterTopic,
            AtomicReference<DefaultErrorHandler> handler) {
        // Same partition number as the source record: per-key grouping is kept in the dead-letter topic.
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(
                template, (record, failure) -> new TopicPartition(deadLetterTopic, record.partition()));
        publisher.setFailIfSendResultIsError(true);
        publisher.setWaitForSendResultTimeout(DEAD_LETTER_SEND_TIMEOUT);
        publisher.excludeHeader(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE);
        publisher.setHeadersFunction((record, failure) -> {
            RecordHeaders headers = new RecordHeaders();
            headers.add(
                    DeadLetterHeaders.FAILURE_CATEGORY,
                    FailureClassifier.classify(failure).name().getBytes(UTF_8));
            // During recovery the error handler reports the number of the *next* attempt, so the attempts made
            // are one fewer (verified by DeadLetterIT and RetryIT: 1 for a bad record, 3 for an exhausted one).
            int attempts = handler.get()
                            .deliveryAttempt(
                                    new TopicPartitionOffset(record.topic(), record.partition(), record.offset()))
                    - 1;
            headers.add(
                    DeadLetterHeaders.DELIVERY_ATTEMPTS,
                    Integer.toString(attempts).getBytes(UTF_8));
            return headers;
        });
        return publisher;
    }

    static BackOff backOff(FailureCategory category, Retry retry) {
        return switch (category) {
            case BAD_RECORD -> new FixedBackOff(0, 0);
            case TRANSIENT -> {
                ExponentialBackOff backOff =
                        new ExponentialBackOff(retry.transientInitialBackoff().toMillis(), 2.0);
                backOff.setMaxInterval(retry.transientMaxBackoff().toMillis());
                backOff.setMaxAttempts(retry.transientMaxAttempts() - 1);
                yield backOff;
            }
            case INFRASTRUCTURE -> unlimited(retry);
        };
    }

    private static BackOff unlimited(Retry retry) {
        ExponentialBackOff backOff =
                new ExponentialBackOff(retry.infrastructureInitialBackoff().toMillis(), 2.0);
        backOff.setMaxInterval(retry.infrastructureMaxBackoff().toMillis());
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }

    /** Counts consumer-level errors (for example an out-of-range position) before the default handling. */
    static final class ClassifiedErrorHandler extends DefaultErrorHandler {

        private final WorkerMetrics metrics;

        ClassifiedErrorHandler(ConsumerRecordRecoverer recoverer, WorkerMetrics metrics) {
            super(recoverer);
            this.metrics = metrics;
        }

        @Override
        public void handleOtherException(
                Exception thrownException,
                Consumer<?, ?> consumer,
                MessageListenerContainer container,
                boolean batchListener) {
            metrics.consumerErrors.increment();
            log.error("Consumer error on {}: {}", container.getListenerId(), thrownException.toString());
            super.handleOtherException(thrownException, consumer, container, batchListener);
        }
    }

    private record MetricsRetryListener(WorkerMetrics metrics) implements RetryListener {

        @Override
        public void failedDelivery(ConsumerRecord<?, ?> record, Exception failure, int deliveryAttempt) {
            FailureCategory category = FailureClassifier.classify(failure);
            metrics.failedAttempt(category);
            log.warn(
                    "Delivery attempt {} of {}-{}@{} failed ({}): {}",
                    deliveryAttempt,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    category,
                    rootMessage(failure));
        }

        @Override
        public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
            metrics.deadLetterPublishFailures.increment();
            log.error(
                    "Dead-letter publication of {}-{}@{} failed; the record will be redelivered: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    failure.toString());
        }

        static String rootMessage(Throwable failure) {
            Throwable root = failure;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            return root.toString();
        }
    }
}
