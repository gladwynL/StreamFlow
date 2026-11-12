package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.processing.BatchEventProcessor;
import io.github.gladwynl.streamflow.processing.BatchEventProcessor.BatchResult;
import io.github.gladwynl.streamflow.processing.BatchEventProcessor.RecordFailedException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.stereotype.Component;

/**
 * Batch mode (ADR-0013): feeds each poll batch of the event topic to the {@link BatchEventProcessor}. A failed record
 * is reported to the error handler by index, so only the offsets of the committed records before it are committed.
 */
@Component
@Profile("worker")
@ConditionalOnProperty(name = "streamflow.worker.processing-mode", havingValue = "BATCH")
class BatchEventListener {

    private final BatchEventProcessor processor;
    private final WorkerMetrics metrics;
    private final WorkerKafkaConfiguration.ClassifiedErrorHandler errorHandler;

    BatchEventListener(
            BatchEventProcessor processor,
            WorkerMetrics metrics,
            WorkerKafkaConfiguration.ClassifiedErrorHandler eventErrorHandler) {
        this.processor = processor;
        this.metrics = metrics;
        this.errorHandler = eventErrorHandler;
    }

    @KafkaListener(
            id = EventListener.LISTENER_ID,
            topics = "#{@topicCatalog.events().name()}",
            groupId = "${streamflow.worker.consumer-group:streamflow-processor}",
            containerFactory = WorkerKafkaConfiguration.EVENT_BATCH_CONTAINER_FACTORY)
    void onEvents(List<ConsumerRecord<String, byte[]>> records) {
        try {
            count(processor.process(records, this::deliveryAttempt));
            metrics.succeeded();
        } catch (RecordFailedException e) {
            count(e.committedBefore());
            throw new BatchListenerFailedException(e.getMessage(), e.getCause(), e.index());
        }
    }

    /** The attempt number the error handler's failure tracker holds for the record (1 if it never failed). */
    private int deliveryAttempt(ConsumerRecord<String, byte[]> record) {
        return Math.max(
                1,
                errorHandler.deliveryAttempt(
                        new TopicPartitionOffset(record.topic(), record.partition(), record.offset())));
    }

    private void count(BatchResult result) {
        metrics.processed.increment(result.processed());
        metrics.duplicates.increment(result.duplicates());
    }
}
