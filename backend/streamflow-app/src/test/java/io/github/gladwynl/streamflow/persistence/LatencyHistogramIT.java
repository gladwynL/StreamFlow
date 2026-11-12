package io.github.gladwynl.streamflow.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.persistence.BatchEventStore.Item;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The latency histogram is bucketed in SQL inside the processing transaction, by both processing modes; this checks
 * that the SQL bucketing agrees with {@link LatencyBuckets} (which computes the percentiles), and that the histogram
 * query sums it.
 */
class LatencyHistogramIT {

    static {
        IntegrationTestInfrastructure.provision(Pipeline.named("latency"));
    }

    /** Partition numbers no real topic has, so rows of other tests never mix in. */
    private static final int RECORD_PARTITION = 9_001;

    private static final int BATCH_PARTITION = 9_002;

    // Each sample sits at least 20% below its bucket's upper bound, so the time between reading the database clock
    // and the insert (a few milliseconds, more on a cold connection) cannot move it across a bound. Exact bound
    // behaviour (upper-inclusive) is covered by LatencyBucketsTest.
    private static final List<Long> LATENCIES = List.of(160L, 400L, 4_000L, 12_000L, 90_000L);

    private final JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();
    private final EventCodec codec = new EventCodec();

    @Test
    void recordModeBucketsMatchTheJavaBuckets() {
        clear(RECORD_PARTITION);
        EventStore store = new EventStore(jdbc);
        List<EventEnvelope> events = TestRecords.events(UUID.randomUUID(), 51, LATENCIES.size());
        Instant now = databaseNow();
        for (int i = 0; i < LATENCIES.size(); i++) {
            EventEnvelope event = events.get(i);
            store.applyFirstDelivery(
                    event,
                    codec.encodePayload(event.payload()),
                    now.minusMillis(LATENCIES.get(i)),
                    RECORD_PARTITION,
                    i);
        }
        assertBuckets(RECORD_PARTITION);
    }

    @Test
    void batchModeBucketsMatchTheJavaBuckets() {
        clear(BATCH_PARTITION);
        BatchEventStore store = new BatchEventStore(new JdbcTemplate(IntegrationTestInfrastructure.dataSource()));
        List<EventEnvelope> events = TestRecords.events(UUID.randomUUID(), 52, LATENCIES.size());
        Instant now = databaseNow();
        List<Item> items = new ArrayList<>();
        for (int i = 0; i < LATENCIES.size(); i++) {
            EventEnvelope event = events.get(i);
            items.add(new Item(
                    event,
                    codec.encodePayload(event.payload()),
                    now.minusMillis(LATENCIES.get(i)),
                    BATCH_PARTITION,
                    i));
        }
        store.addLatency(items);
        assertBuckets(BATCH_PARTITION);
    }

    private void assertBuckets(int partition) {
        Map<Integer, Long> stored = jdbc
                .sql("SELECT bucket_index, sum(event_count) AS n FROM latency_stats_minute"
                        + " WHERE kafka_partition = :p GROUP BY bucket_index")
                .param("p", partition)
                .query((rs, rowNum) -> Map.entry(rs.getInt("bucket_index"), rs.getLong("n")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Integer, Long> expected =
                LATENCIES.stream().collect(Collectors.groupingBy(LatencyBuckets::indexOf, Collectors.counting()));
        assertThat(stored).isEqualTo(expected);
        assertThat(stored).containsKey(LatencyBuckets.BUCKETS - 1); // 90 s lands in the open-ended bucket

        long[] histogram = new DashboardQueries(jdbc).latencyHistogram(Duration.ofMinutes(5));
        for (Map.Entry<Integer, Long> bucket : expected.entrySet()) {
            assertThat(histogram[bucket.getKey()]).isGreaterThanOrEqualTo(bucket.getValue());
        }
        clear(partition);
    }

    private Instant databaseNow() {
        return jdbc.sql("SELECT now()").query(java.sql.Timestamp.class).single().toInstant();
    }

    private void clear(int partition) {
        jdbc.sql("DELETE FROM latency_stats_minute WHERE kafka_partition = :p")
                .param("p", partition)
                .update();
    }
}
