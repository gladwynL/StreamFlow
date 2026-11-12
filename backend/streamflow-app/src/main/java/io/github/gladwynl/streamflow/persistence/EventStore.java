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

    /**
     * Writes every effect of a first delivery — the event row, its per-minute aggregate, its latency histogram bucket
     * and the run statistics — in one statement (data-modifying CTEs), so the record costs one round trip here
     * instead of four. Same tables, keys, conflict handling and transaction as separate statements; all parts see the
     * same snapshot and {@code now()}. Latency is the transaction start minus the Kafka append time (database clock),
     * counted into the bucket whose bounds below it number {@code bucket_index}.
     */
    public void applyFirstDelivery(
            EventEnvelope event, String payloadJson, Instant kafkaTimestamp, int partition, long offset) {
        Long amount = amountCents(event.payload());
        jdbc.sql("""
                        WITH event_row AS (
                            INSERT INTO events (event_id, event_type, occurred_at, kafka_ts, kafka_partition,
                                                kafka_offset, run_id, run_seq, user_id, session_id, synthetic,
                                                amount_cents, currency, payload)
                            VALUES (:eventId, :eventType, :occurredAt, :kafkaTs, :partition, :offset, :runId, :runSeq,
                                    :userId, :sessionId, :synthetic, :amountCents, :currency, CAST(:payload AS jsonb))),
                        minute_stats AS (
                            INSERT INTO event_stats_minute (bucket_start, event_type, kafka_partition, event_count,
                                                            amount_cents)
                            VALUES (date_trunc('minute', now()), :eventType, :partition, 1, :amountTotal)
                            ON CONFLICT (bucket_start, event_type, kafka_partition) DO UPDATE
                            SET event_count = event_stats_minute.event_count + 1,
                                amount_cents = event_stats_minute.amount_cents + EXCLUDED.amount_cents),
                        latency AS (
                            INSERT INTO latency_stats_minute (bucket_start, kafka_partition, bucket_index, event_count)
                            SELECT date_trunc('minute', now()), :partition,
                                   (SELECT count(*) FROM unnest(%s) AS bound WHERE bound < sample.latency_ms), 1
                            FROM (SELECT greatest(0, floor(extract(epoch FROM now() - :kafkaTs) * 1000))::bigint
                                         AS latency_ms) sample
                            ON CONFLICT (bucket_start, kafka_partition, bucket_index) DO UPDATE
                            SET event_count = latency_stats_minute.event_count + 1)
                        INSERT INTO run_stats (run_id, kafka_partition, processed, duplicates_suppressed)
                        VALUES (:runId, :partition, 1, 0)
                        ON CONFLICT (run_id, kafka_partition) DO UPDATE
                        SET processed = run_stats.processed + 1
                        """.formatted(LATENCY_BOUNDS_SQL))
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
                .param("amountCents", amount)
                .param("amountTotal", amount == null ? 0L : amount)
                .param("currency", currency(event.payload()))
                .param("payload", payloadJson)
                .update();
    }

    static final String LATENCY_BOUNDS_SQL = "ARRAY["
            + java.util.Arrays.stream(LatencyBuckets.UPPER_BOUNDS)
                    .mapToObj(Long::toString)
                    .collect(java.util.stream.Collectors.joining(","))
            + "]::bigint[]";

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
