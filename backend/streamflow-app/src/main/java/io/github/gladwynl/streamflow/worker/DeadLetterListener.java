package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.deadletter.DeadLetterIngestion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Copies every record of the dead-letter topic into PostgreSQL ({@link DeadLetterIngestion}). */
@Component
@Profile("worker")
class DeadLetterListener {

    static final String LISTENER_ID = "dead-letter-ingestor";

    private final DeadLetterIngestion ingestion;
    private final WorkerMetrics metrics;

    DeadLetterListener(DeadLetterIngestion ingestion, WorkerMetrics metrics) {
        this.ingestion = ingestion;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = "#{@topicCatalog.deadLetter().name()}",
            groupId = "${streamflow.dead-letters.ingestor-group:streamflow-dlt-ingestor}",
            containerFactory = WorkerKafkaConfiguration.DEAD_LETTER_CONTAINER_FACTORY)
    void onDeadLetter(ConsumerRecord<String, byte[]> record) {
        (ingestion.ingest(record) ? metrics.ingested : metrics.ingestedDuplicates).increment();
    }
}
