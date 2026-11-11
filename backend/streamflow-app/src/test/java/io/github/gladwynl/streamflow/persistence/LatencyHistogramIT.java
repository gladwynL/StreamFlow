package io.github.gladwynl.streamflow.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The latency histogram is bucketed in SQL inside the processing transaction; this checks that the SQL bucketing
 * agrees with {@link LatencyBuckets} (which computes the percentiles), and that the histogram query sums it.
 */
class LatencyHistogramIT {

    static {
        IntegrationTestInfrastructure.provision(Pipeline.named("latency"));
    }

    /** A partition number no real topic has, so rows of other tests never mix in. */
    private static final int PARTITION = 9_001;

    private final JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();
    private final EventStore store = new EventStore(jdbc);
    private final DashboardQueries queries = new DashboardQueries(jdbc);

    @Test
    void sqlBucketsMatchTheJavaBuckets() {
        jdbc.sql("DELETE FROM latency_stats_minute WHERE kafka_partition = :p")
                .param("p", PARTITION)
                .update();
        // Each sample sits at least 20% below its bucket's upper bound, so the time between reading the database
        // clock and the insert (a few milliseconds, more on a cold connection) cannot move it across a bound.
        // Exact bound behaviour (upper-inclusive) is covered by LatencyBucketsTest.
        List<Long> latencies = List.of(160L, 400L, 4_000L, 12_000L, 90_000L);
        Instant now = jdbc.sql("SELECT now()")
                .query(java.sql.Timestamp.class)
                .single()
                .toInstant();
        for (long millis : latencies) {
            store.addLatency(now.minusMillis(millis), PARTITION);
        }

        Map<Integer, Long> stored = jdbc
                .sql("SELECT bucket_index, sum(event_count) AS n FROM latency_stats_minute"
                        + " WHERE kafka_partition = :p GROUP BY bucket_index")
                .param("p", PARTITION)
                .query((rs, rowNum) -> Map.entry(rs.getInt("bucket_index"), rs.getLong("n")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<Integer, Long> expected =
                latencies.stream().collect(Collectors.groupingBy(LatencyBuckets::indexOf, Collectors.counting()));
        assertThat(stored).isEqualTo(expected);
        assertThat(stored).containsKey(LatencyBuckets.BUCKETS - 1); // 90 s lands in the open-ended bucket

        long[] histogram = queries.latencyHistogram(Duration.ofMinutes(5));
        for (Map.Entry<Integer, Long> bucket : expected.entrySet()) {
            assertThat(histogram[bucket.getKey()]).isGreaterThanOrEqualTo(bucket.getValue());
        }

        jdbc.sql("DELETE FROM latency_stats_minute WHERE kafka_partition = :p")
                .param("p", PARTITION)
                .update();
    }
}
