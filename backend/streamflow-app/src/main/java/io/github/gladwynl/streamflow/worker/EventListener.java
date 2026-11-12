package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.processing.EventProcessor;
import io.github.gladwynl.streamflow.processing.ProcessingResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Record mode: feeds each record of the event topic to the {@link EventProcessor}. */
@Component
@Profile("worker")
@ConditionalOnProperty(name = "streamflow.worker.processing-mode", havingValue = "RECORD")
class EventListener {

    static final String LISTENER_ID = "event-processor";

    private final EventProcessor processor;
    private final WorkerMetrics metrics;

    EventListener(EventProcessor processor, WorkerMetrics metrics) {
        this.processor = processor;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = "#{@topicCatalog.events().name()}",
            groupId = "${streamflow.worker.consumer-group:streamflow-processor}",
            containerFactory = WorkerKafkaConfiguration.EVENT_CONTAINER_FACTORY)
    void onEvent(ConsumerRecord<String, byte[]> record, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int deliveryAttempt) {
        ProcessingResult result = processor.process(record, deliveryAttempt);
        (result == ProcessingResult.PROCESSED ? metrics.processed : metrics.duplicates).increment();
        metrics.succeeded();
    }
}
