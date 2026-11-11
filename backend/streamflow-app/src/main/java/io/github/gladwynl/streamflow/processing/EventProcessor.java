package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventValidator;
import io.github.gladwynl.streamflow.persistence.EventStore;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Processes one Kafka record in one database transaction.
 *
 * <p>The ledger insert and every effect (event row, minute aggregate, run statistics) commit or roll back together.
 * A record whose event id is already in the ledger changes nothing except the duplicate counter. The transaction has
 * committed when this method returns; the listener container commits the Kafka offset only afterwards (ADR-0005).
 *
 * <p>Failures propagate unchanged; the listener container's error handler classifies them ({@link
 * FailureClassifier}) and retries, dead-letters or waits accordingly.
 */
@Component
public class EventProcessor {

    private final EventCodec codec;
    private final EventValidator validator;
    private final EventStore store;
    private final TransactionTemplate transaction;
    private final CrashInjector crashInjector;
    private final FaultInjector faultInjector;

    public EventProcessor(
            EventCodec codec,
            EventValidator validator,
            EventStore store,
            PlatformTransactionManager transactionManager,
            CrashInjector crashInjector,
            FaultInjector faultInjector) {
        this.codec = codec;
        this.validator = validator;
        this.store = store;
        this.transaction = new TransactionTemplate(transactionManager);
        this.crashInjector = crashInjector;
        this.faultInjector = faultInjector;
    }

    public ProcessingResult process(ConsumerRecord<String, byte[]> record) {
        return process(record, 1);
    }

    /**
     * @param deliveryAttempt 1 on the first delivery to this consumer, incremented by each in-place retry
     */
    public ProcessingResult process(ConsumerRecord<String, byte[]> record, int deliveryAttempt) {
        EventEnvelope event = codec.decode(record.value());
        validator.requireValid(event);
        String payloadJson = codec.encodePayload(event.payload());
        int partition = record.partition();

        ProcessingResult result = transaction.execute(status -> {
            boolean firstDelivery = store.recordInLedger(event, partition, record.offset());
            if (firstDelivery) {
                // Injected faults roll back the ledger row like any real failure; duplicates are never failed.
                faultInjector.apply(event, deliveryAttempt);
                store.insertEvent(
                        event, payloadJson, Instant.ofEpochMilli(record.timestamp()), partition, record.offset());
                store.addToMinuteStats(event, partition);
                store.countProcessed(event.runId(), partition);
            } else {
                store.countDuplicate(event.runId(), partition);
            }
            crashInjector.beforeCommit(record, event);
            return firstDelivery ? ProcessingResult.PROCESSED : ProcessingResult.DUPLICATE;
        });
        crashInjector.afterCommit(record, event);
        return result;
    }
}
