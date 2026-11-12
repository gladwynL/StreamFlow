package io.github.gladwynl.streamflow.persistence;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventPayload;
import io.github.gladwynl.streamflow.events.EventPayload.PaymentFailed;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Set-based writes of the batch processing mode (ADR-0013). Same tables, keys and effects as {@link EventStore},
 * with one statement per table for a whole poll batch instead of one per record. Every method must run inside the
 * caller's transaction: the ledger rows and all effects of the batch commit or roll back together.
 *
 * <p>Rows are written in a fixed order (by event id, then by aggregate key) so concurrent batches acquire row locks
 * in the same order.
 */
@Repository
public class BatchEventStore {

    /** One record of a batch, decoded and validated. */
    public record Item(EventEnvelope event, String payloadJson, Instant kafkaTimestamp, int partition, long offset) {}

    private final JdbcTemplate jdbc;

    public BatchEventStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the batch into the deduplication ledger and returns the event ids that were new. Ids already in the
     * ledger, or repeated within the batch, are not returned.
     */
    public Set<UUID> recordInLedger(List<Item> items) {
        List<Item> sorted = new ArrayList<>(items);
        sorted.sort(Comparator.comparing(i -> i.event().eventId()));
        Set<UUID> inserted = new HashSet<>();
        jdbc.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement("""
                            INSERT INTO processed_events (event_id, run_id, run_seq, kafka_partition, kafka_offset)
                            SELECT * FROM unnest(?::uuid[], ?::uuid[], ?::bigint[], ?::int[], ?::bigint[])
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """);
                    ps.setArray(
                            1,
                            array(
                                    connection,
                                    "uuid",
                                    sorted.stream().map(i -> i.event().eventId())));
                    ps.setArray(
                            2,
                            array(
                                    connection,
                                    "uuid",
                                    sorted.stream().map(i -> i.event().runId())));
                    ps.setArray(
                            3,
                            array(
                                    connection,
                                    "bigint",
                                    sorted.stream().map(i -> i.event().sequence())));
                    ps.setArray(4, array(connection, "int", sorted.stream().map(Item::partition)));
                    ps.setArray(5, array(connection, "bigint", sorted.stream().map(Item::offset)));
                    return ps;
                },
                rs -> {
                    inserted.add(rs.getObject(1, UUID.class));
                });
        return inserted;
    }

    /** Inserts the event rows of the batch's first deliveries. */
    public void insertEvents(List<Item> items) {
        if (items.isEmpty()) {
            return;
        }
        jdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO events (event_id, event_type, occurred_at, kafka_ts, kafka_partition, kafka_offset,
                                        run_id, run_seq, user_id, session_id, synthetic, amount_cents, currency,
                                        payload)
                    SELECT e, t, o, k, p, off, r, s, u, ses, syn, a, c, CAST(pl AS jsonb)
                    FROM unnest(?::uuid[], ?::text[], ?::timestamptz[], ?::timestamptz[], ?::int[], ?::bigint[],
                                ?::uuid[], ?::bigint[], ?::text[], ?::text[], ?::boolean[], ?::bigint[], ?::text[],
                                ?::text[])
                         AS x(e, t, o, k, p, off, r, s, u, ses, syn, a, c, pl)
                    """);
            ps.setArray(
                    1,
                    array(connection, "uuid", items.stream().map(i -> i.event().eventId())));
            ps.setArray(
                    2,
                    array(
                            connection,
                            "text",
                            items.stream().map(i -> i.event().eventType().name())));
            ps.setArray(
                    3,
                    array(
                            connection,
                            "timestamptz",
                            items.stream().map(i -> Timestamp.from(i.event().occurredAt()))));
            ps.setArray(
                    4, array(connection, "timestamptz", items.stream().map(i -> Timestamp.from(i.kafkaTimestamp()))));
            ps.setArray(5, array(connection, "int", items.stream().map(Item::partition)));
            ps.setArray(6, array(connection, "bigint", items.stream().map(Item::offset)));
            ps.setArray(
                    7,
                    array(connection, "uuid", items.stream().map(i -> i.event().runId())));
            ps.setArray(
                    8,
                    array(
                            connection,
                            "bigint",
                            items.stream().map(i -> i.event().sequence())));
            ps.setArray(
                    9,
                    array(connection, "text", items.stream().map(i -> i.event().userId())));
            ps.setArray(
                    10,
                    array(connection, "text", items.stream().map(i -> i.event().sessionId())));
            ps.setArray(
                    11,
                    array(
                            connection,
                            "boolean",
                            items.stream().map(i -> i.event().synthetic())));
            ps.setArray(
                    12,
                    array(
                            connection,
                            "bigint",
                            items.stream().map(i -> amountCents(i.event().payload()))));
            ps.setArray(
                    13,
                    array(
                            connection,
                            "text",
                            items.stream().map(i -> currency(i.event().payload()))));
            ps.setArray(14, array(connection, "text", items.stream().map(Item::payloadJson)));
            return ps;
        });
    }

    /** Adds the first deliveries to the per-minute, per-type, per-partition aggregates. */
    public void addToMinuteStats(List<Item> items) {
        Map<String, long[]> totals = new TreeMap<>();
        for (Item item : items) {
            Long amount = amountCents(item.event().payload());
            long[] t =
                    totals.computeIfAbsent(item.event().eventType().name() + "|" + item.partition(), k -> new long[2]);
            t[0]++;
            t[1] += amount == null ? 0 : amount;
        }
        if (totals.isEmpty()) {
            return;
        }
        List<String> types = new ArrayList<>();
        List<Integer> partitions = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        List<Long> amounts = new ArrayList<>();
        totals.forEach((key, t) -> {
            int bar = key.indexOf('|');
            types.add(key.substring(0, bar));
            partitions.add(Integer.parseInt(key.substring(bar + 1)));
            counts.add(t[0]);
            amounts.add(t[1]);
        });
        jdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO event_stats_minute (bucket_start, event_type, kafka_partition, event_count, amount_cents)
                    SELECT date_trunc('minute', now()), t, p, n, a
                    FROM unnest(?::text[], ?::int[], ?::bigint[], ?::bigint[]) AS x(t, p, n, a)
                    ON CONFLICT (bucket_start, event_type, kafka_partition) DO UPDATE
                    SET event_count = event_stats_minute.event_count + EXCLUDED.event_count,
                        amount_cents = event_stats_minute.amount_cents + EXCLUDED.amount_cents
                    """);
            ps.setArray(1, array(connection, "text", types.stream()));
            ps.setArray(2, array(connection, "int", partitions.stream()));
            ps.setArray(3, array(connection, "bigint", counts.stream()));
            ps.setArray(4, array(connection, "bigint", amounts.stream()));
            return ps;
        });
    }

    /**
     * Adds the first deliveries to the latency histogram. As in record mode, latency is measured from the Kafka
     * append time to the start of the processing transaction (database clock), here the batch's transaction.
     */
    public void addLatency(List<Item> items) {
        if (items.isEmpty()) {
            return;
        }
        jdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    WITH sample AS (
                        SELECT p, (SELECT count(*) FROM unnest(%s) AS bound
                                   WHERE bound < greatest(0, floor(extract(epoch FROM now() - k) * 1000))) AS idx
                        FROM unnest(?::int[], ?::timestamptz[]) AS x(p, k))
                    INSERT INTO latency_stats_minute (bucket_start, kafka_partition, bucket_index, event_count)
                    SELECT date_trunc('minute', now()), p, idx, count(*) FROM sample
                    GROUP BY p, idx ORDER BY p, idx
                    ON CONFLICT (bucket_start, kafka_partition, bucket_index) DO UPDATE
                    SET event_count = latency_stats_minute.event_count + EXCLUDED.event_count
                    """.formatted(EventStore.LATENCY_BOUNDS_SQL));
            ps.setArray(1, array(connection, "int", items.stream().map(Item::partition)));
            ps.setArray(
                    2, array(connection, "timestamptz", items.stream().map(i -> Timestamp.from(i.kafkaTimestamp()))));
            return ps;
        });
    }

    /** Adds processed (first deliveries) and suppressed duplicates to the per-run, per-partition statistics. */
    public void addRunStats(List<Item> all, Set<UUID> firstDeliveries) {
        Map<String, long[]> totals = new TreeMap<>();
        Set<UUID> counted = new HashSet<>();
        for (Item item : all) {
            long[] t = totals.computeIfAbsent(item.event().runId() + "|" + item.partition(), k -> new long[2]);
            // An event new to the ledger counts as processed once; any further copy in the batch is a duplicate.
            if (firstDeliveries.contains(item.event().eventId())
                    && counted.add(item.event().eventId())) {
                t[0]++;
            } else {
                t[1]++;
            }
        }
        List<UUID> runs = new ArrayList<>();
        List<Integer> partitions = new ArrayList<>();
        List<Long> processed = new ArrayList<>();
        List<Long> duplicates = new ArrayList<>();
        totals.forEach((key, t) -> {
            int bar = key.indexOf('|');
            runs.add(UUID.fromString(key.substring(0, bar)));
            partitions.add(Integer.parseInt(key.substring(bar + 1)));
            processed.add(t[0]);
            duplicates.add(t[1]);
        });
        jdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO run_stats (run_id, kafka_partition, processed, duplicates_suppressed)
                    SELECT * FROM unnest(?::uuid[], ?::int[], ?::bigint[], ?::bigint[])
                    ON CONFLICT (run_id, kafka_partition) DO UPDATE
                    SET processed = run_stats.processed + EXCLUDED.processed,
                        duplicates_suppressed = run_stats.duplicates_suppressed + EXCLUDED.duplicates_suppressed
                    """);
            ps.setArray(1, array(connection, "uuid", runs.stream()));
            ps.setArray(2, array(connection, "int", partitions.stream()));
            ps.setArray(3, array(connection, "bigint", processed.stream()));
            ps.setArray(4, array(connection, "bigint", duplicates.stream()));
            return ps;
        });
    }

    private static Array array(Connection connection, String type, java.util.stream.Stream<?> values)
            throws SQLException {
        return connection.createArrayOf(type, values.toArray());
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
