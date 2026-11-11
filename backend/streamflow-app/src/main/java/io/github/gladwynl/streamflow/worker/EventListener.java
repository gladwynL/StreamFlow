package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.processing.EventProcessor;
import io.github.gladwynl.streamflow.processing.ProcessingResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Feeds each record of the event topic to the {@link EventProcessor}. */
@Component
@Profile("worker")
class EventListener {

    static final String LISTENER_ID = "event-processor";

    private final EventProcessor processor;
    private final Counter processed;
    private final Counter duplicates;

    EventListener(EventProcessor processor, MeterRegistry meters) {
        this.processor = processor;
        this.processed = meters.counter("streamflow.events.processed");
        this.duplicates = meters.counter("streamflow.events.duplicates.suppressed");
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = "#{@topicCatalog.events().name()}",
            groupId = "${streamflow.worker.consumer-group:streamflow-processor}",
            containerFactory = WorkerKafkaConfiguration.CONTAINER_FACTORY)
    void onEvent(ConsumerRecord<String, byte[]> record) {
        ProcessingResult result = processor.process(record);
        (result == ProcessingResult.PROCESSED ? processed : duplicates).increment();
    }
}
