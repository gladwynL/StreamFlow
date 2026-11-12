package io.github.gladwynl.streamflow.api.live;

import io.github.gladwynl.streamflow.persistence.DashboardQueries.DeadLetterRow;
import io.github.gladwynl.streamflow.persistence.DashboardQueries.EventRow;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The live state pushed to dashboards about once per second (WebSocket) and served on demand (REST). Every value
 * is a measurement; a value that could not be measured is {@code null}, never estimated.
 *
 * @param sequence increases by one per snapshot computed by this api process
 * @param databaseError set when PostgreSQL could not be queried; throughput, latency, workers, dead-letter counts
 *     and recent rows are then null
 */
public record LiveSnapshot(
        long sequence,
        Instant at,
        Throughput throughput,
        Latency latency,
        Kafka kafka,
        String databaseError,
        Workers workers,
        Map<String, Long> deadLetters,
        ActiveRun activeRun,
        List<EventRow> recentEvents,
        List<DeadLetterRow> recentDeadLetters) {

    /**
     * @param publishedPerSecond records appended to the event topic per second, from Kafka log-end offsets
     *     (null on the first sample)
     * @param processedPerSecond events applied (first deliveries) per second, averaged over the last 10 s
     * @param processedLastMinute events applied in the last 60 s
     * @param deadLetteredLastMinute dead letters stored in the last 60 s
     * @param failedAttemptsPerSecond failed processing attempts per second across live workers (10 s window;
     *     includes attempts that later succeeded), by failure category
     * @param duplicatesPerSecond deliveries suppressed by the ledger per second across live workers (10 s window)
     */
    public record Throughput(
            Double publishedPerSecond,
            double processedPerSecond,
            long processedLastMinute,
            long deadLetteredLastMinute,
            Map<String, Double> failedAttemptsPerSecond,
            Double duplicatesPerSecond) {}

    /**
     * Processing latency (Kafka append to database transaction start) over the last 5 minutes, from the latency
     * histogram. Each percentile is the upper bound of its histogram bucket; null when there were no events.
     */
    public record Latency(long samples, Long p50Millis, Long p95Millis, Long p99Millis) {}

    /**
     * @param retentionBytes the event topic's size limit per partition ({@code retention.bytes}); beyond it Kafka
     *     deletes the oldest records, read or not
     * @param error set when Kafka could not be queried; the other fields are then null
     */
    public record Kafka(
            String groupState,
            Integer members,
            Long totalLag,
            Long logEndOffset,
            Long retentionBytes,
            List<PartitionView> partitions,
            String error) {}

    /** @param sizeBytes bytes the partition occupies on the broker, or null if they could not be read */
    public record PartitionView(
            int partition, long logEndOffset, Long committedOffset, long lag, String owner, Long sizeBytes) {}

    public record Workers(int live, int seen, boolean anyInOutage, List<WorkerView> list) {}

    public record WorkerView(
            UUID workerId,
            String host,
            boolean live,
            Instant lastSeenAt,
            long processed,
            long duplicates,
            long failedAttempts,
            long deadLettered,
            boolean inOutage,
            List<Integer> partitions) {}

    /**
     * @param phase PUBLISHING, DRAINING (waiting for the workers before reconciling) or RECONCILED
     * @param processed distinct events of the run applied so far (null when the database is unavailable)
     */
    public record ActiveRun(
            UUID runId,
            String phase,
            long requestedEvents,
            int ratePerSecond,
            long attempted,
            long acked,
            long failed,
            Long processed,
            Instant startedAt) {}
}
