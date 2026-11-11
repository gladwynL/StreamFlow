package io.github.gladwynl.streamflow.persistence;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventPayload;
import io.github.gladwynl.streamflow.events.EventPayload.PaymentFailed;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Writes of the processing path. Every method must run inside the caller's transaction: the ledger row and the
 * effects of one event commit or roll back together. Database time ({@code now()}, the transaction start) is used
 * for ingestion timestamps and aggregate buckets.
 */
@Repository
public class EventStore {

    private final JdbcClient jdbc;

    public EventStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Records the event in the deduplication ledger. Returns {@code false} if it was already processed. */
    public boolean recordInLedger(EventEnvelope event, int partition, long offset) {
        return jdbc.sql("""
                        INSERT INTO processed_events (event_id, run_id, run_seq, kafka_partition, kafka_offset)
                        VALUES (:eventId, :runId, :runSeq, :partition, :offset)
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                        .param("eventId", event.eventId())
                        .param("runId", event.runId())
                        .param("runSeq", event.sequence())
                        .param("partition", partition)
                        .param("offset", offset)
                        .update()
                == 1;
    }

    public void insertEvent(
            EventEnvelope event, String payloadJson, Instant kafkaTimestamp, int partition, long offset) {
        jdbc.sql("""
                        INSERT INTO events (event_id, event_type, occurred_at, kafka_ts, kafka_partition, kafka_offset,
                                            run_id, run_seq, user_id, session_id, synthetic, amount_cents, currency,
                                            payload)
                        VALUES (:eventId, :eventType, :occurredAt, :kafkaTs, :partition, :offset,
                                :runId, :runSeq, :userId, :sessionId, :synthetic, :amountCents, :currency,
                                CAST(:payload AS jsonb))
                        """)
                .param("eventId", event.eventId())
                .param("eventType", event.eventType().name())
                .param("occurredAt", Timestamp.from(event.occurredAt()))
                .param("kafkaTs", Timestamp.from(kafkaTimestamp))
                .param("partition", partition)
                .param("offset", offset)
                .param("runId", event.runId())
                .param("runSeq", event.sequence())
                .param("userId", event.userId())
                .param("sessionId", event.sessionId())
                .param("synthetic", event.synthetic())
                .param("amountCents", amountCents(event.payload()))
                .param("currency", currency(event.payload()))
                .param("payload", payloadJson)
                .update();
    }

    /** Adds the event to its per-minute, per-type, per-partition aggregate. */
    public void addToMinuteStats(EventEnvelope event, int partition) {
        Long amount = amountCents(event.payload());
        jdbc.sql("""
                        INSERT INTO event_stats_minute (bucket_start, event_type, kafka_partition, event_count, amount_cents)
                        VALUES (date_trunc('minute', now()), :eventType, :partition, 1, :amount)
                        ON CONFLICT (bucket_start, event_type, kafka_partition) DO UPDATE
                        SET event_count = event_stats_minute.event_count + 1,
                            amount_cents = event_stats_minute.amount_cents + EXCLUDED.amount_cents
                        """)
                .param("eventType", event.eventType().name())
                .param("partition", partition)
                .param("amount", amount == null ? 0L : amount)
                .update();
    }

    public void countProcessed(UUID runId, int partition) {
        upsertRunStats(runId, partition, 1, 0);
    }

    public void countDuplicate(UUID runId, int partition) {
        upsertRunStats(runId, partition, 0, 1);
    }

    private void upsertRunStats(UUID runId, int partition, int processed, int duplicates) {
        jdbc.sql("""
                        INSERT INTO run_stats (run_id, kafka_partition, processed, duplicates_suppressed)
                        VALUES (:runId, :partition, :processed, :duplicates)
                        ON CONFLICT (run_id, kafka_partition) DO UPDATE
                        SET processed = run_stats.processed + EXCLUDED.processed,
                            duplicates_suppressed = run_stats.duplicates_suppressed + EXCLUDED.duplicates_suppressed
                        """)
                .param("runId", runId)
                .param("partition", partition)
                .param("processed", processed)
                .param("duplicates", duplicates)
                .update();
    }

    private static Long amountCents(EventPayload payload) {
        return switch (payload) {
            case PurchaseCompleted p -> p.amountCents();
            case PaymentFailed p -> p.amountCents();
            default -> null;
        };
    }

    private static String currency(EventPayload payload) {
        return switch (payload) {
            case PurchaseCompleted p -> p.currency();
            case PaymentFailed p -> p.currency();
            default -> null;
        };
    }
}
