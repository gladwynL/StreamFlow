package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventValidator;
import io.github.gladwynl.streamflow.persistence.BatchEventStore;
import io.github.gladwynl.streamflow.persistence.BatchEventStore.Item;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToIntFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Batch processing mode (ADR-0013): processes a whole poll batch in one database transaction with set-based
 * statements, falling back to record-by-record processing when anything in the batch fails.
 *
 * <ul>
 *   <li><b>Infrastructure failures</b> (database or connection pool unavailable) skip the fallback: nothing has
 *       committed, and the batch is reported failed at its first record so the error handler backs off.
 *   <li><b>Fast path.</b> Every record is decoded and validated, then the ledger rows and all effects of the batch
 *       are written in one transaction. Idempotency is the same as in record mode: only events newly inserted into
 *       the ledger get effects; everything else counts as a duplicate.
 *   <li><b>Fallback.</b> If any record cannot be decoded or validated, or the batch transaction fails for any reason
 *       (injected fault, constraint violation, deadlock, lost connection), the batch transaction is rolled back and
 *       the records are processed one by one by {@link EventProcessor}, each in its own transaction, in offset
 *       order. The first record that fails stops the batch with {@link RecordFailedException} carrying its index:
 *       the records before it have committed, the record itself and those after it have not.
 * </ul>
 *
 * The listener turns {@link RecordFailedException} into Spring Kafka's {@code BatchListenerFailedException}, whose
 * error handler commits only the offsets before the failed record and then retries, dead-letters or waits for that
 * record exactly as in record mode. No offset is committed for a record whose effects are not committed.
 */
@Component
public class BatchEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchEventProcessor.class);

    /** Outcome counts of one batch. */
    public record BatchResult(int processed, int duplicates, boolean fellBack) {}

    /** Record {@code index} of the batch failed; the records before it have been committed. */
    public static class RecordFailedException extends RuntimeException {
        private final int index;
        private final transient BatchResult committedBefore;

        RecordFailedException(int index, BatchResult committedBefore, RuntimeException cause) {
            super("Record " + index + " of the batch failed: " + cause.getMessage(), cause);
            this.index = index;
            this.committedBefore = committedBefore;
        }

        public int index() {
            return index;
        }

        /** Outcomes of the records before the failed one, which have committed. */
        public BatchResult committedBefore() {
            return committedBefore;
        }
    }

    private final EventCodec codec;
    private final EventValidator validator;
    private final BatchEventStore store;
    private final EventProcessor recordProcessor;
    private final TransactionTemplate transaction;
    private final CrashInjector crashInjector;
    private final FaultInjector faultInjector;

    public BatchEventProcessor(
            EventCodec codec,
            EventValidator validator,
            BatchEventStore store,
            EventProcessor recordProcessor,
            PlatformTransactionManager transactionManager,
            CrashInjector crashInjector,
            FaultInjector faultInjector) {
        this.codec = codec;
        this.validator = validator;
        this.store = store;
        this.recordProcessor = recordProcessor;
        this.transaction = new TransactionTemplate(transactionManager);
        this.crashInjector = crashInjector;
        this.faultInjector = faultInjector;
    }

    /**
     * @param deliveryAttempt the delivery attempt of each record (1 unless it failed before), as tracked by the
     *     container's error handler
     */
    public BatchResult process(
            List<ConsumerRecord<String, byte[]>> records,
            ToIntFunction<ConsumerRecord<String, byte[]>> deliveryAttempt) {
        if (records.isEmpty()) {
            return new BatchResult(0, 0, false);
        }
        try {
            return fastPath(records, deliveryAttempt);
        } catch (RuntimeException e) {
            if (FailureClassifier.classify(e) == FailureCategory.INFRASTRUCTURE) {
                // The database (or the pool) is unavailable: processing record by record would only wait for the
                // same outage again. Nothing of the batch has committed; report it from its first record so the
                // error handler backs off and retries, exactly as record mode does during an outage.
                throw new RecordFailedException(0, new BatchResult(0, 0, false), e);
            }
            log.debug("Batch of {} records failed ({}); processing record by record", records.size(), e.toString());
            return recordByRecord(records, deliveryAttempt);
        }
    }

    private BatchResult fastPath(
            List<ConsumerRecord<String, byte[]>> records,
            ToIntFunction<ConsumerRecord<String, byte[]>> deliveryAttempt) {
        List<Item> items = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            EventEnvelope event = codec.decode(record.value());
            validator.requireValid(event);
            items.add(new Item(
                    event,
                    codec.encodePayload(event.payload()),
                    Instant.ofEpochMilli(record.timestamp()),
                    record.partition(),
                    record.offset()));
        }
        BatchResult result = transaction.execute(status -> {
            Set<UUID> firstDeliveries = store.recordInLedger(items);
            List<Item> applied = new ArrayList<>(firstDeliveries.size());
            Set<UUID> seen = new HashSet<>();
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                UUID id = item.event().eventId();
                if (firstDeliveries.contains(id) && seen.add(id)) {
                    // Injected faults roll back the whole batch like any real failure; duplicates are never failed.
                    faultInjector.apply(item.event(), deliveryAttempt.applyAsInt(records.get(i)));
                    applied.add(item);
                }
            }
            store.insertEvents(applied);
            store.addToMinuteStats(applied);
            store.addLatency(applied);
            store.addRunStats(items, firstDeliveries);
            for (int i = 0; i < records.size(); i++) {
                crashInjector.beforeCommit(records.get(i), items.get(i).event());
            }
            return new BatchResult(applied.size(), items.size() - applied.size(), false);
        });
        for (int i = 0; i < records.size(); i++) {
            crashInjector.afterCommit(records.get(i), items.get(i).event());
        }
        return result;
    }

    private BatchResult recordByRecord(
            List<ConsumerRecord<String, byte[]>> records,
            ToIntFunction<ConsumerRecord<String, byte[]>> deliveryAttempt) {
        int processed = 0;
        int duplicates = 0;
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            try {
                if (recordProcessor.process(record, deliveryAttempt.applyAsInt(record)) == ProcessingResult.PROCESSED) {
                    processed++;
                } else {
                    duplicates++;
                }
            } catch (RuntimeException e) {
                throw new RecordFailedException(i, new BatchResult(processed, duplicates, true), e);
            }
        }
        return new BatchResult(processed, duplicates, true);
    }
}
