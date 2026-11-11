package io.github.gladwynl.streamflow.persistence;

import io.github.gladwynl.streamflow.config.StreamflowProperties.Retention;
import java.time.Duration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Retention jobs (ADR-0008). Every step is a conditional statement that is safe to run repeatedly and concurrently.
 *
 * <ul>
 *   <li>Ledger rows are deleted only when older than the ledger retention <em>and</em> their run already has a final
 *       (PASS or FAIL) reconciliation, so deduplication covers every redelivery and replay window and an open run
 *       never loses the evidence it is reconciled against. Rows are deleted in bounded batches.
 *   <li>PENDING dead letters older than the replay window become EXPIRED. Unresolved (PENDING, EXPIRED) dead letters
 *       are never deleted; REPLAYED and DISCARDED ones are deleted after the resolved retention.
 *   <li>Daily {@code events} partitions older than the events retention are dropped; minute aggregates older than
 *       the stats retention are deleted.
 * </ul>
 */
@Repository
public class RetentionMaintenance {

    public static final int LEDGER_BATCH = 10_000;

    public record Result(
            int expiredDeadLetters,
            int deletedDeadLetters,
            long deletedLedgerRows,
            int droppedEventPartitions,
            int deletedStatsRows) {}

    private final JdbcClient jdbc;
    private final DeadLetterRepository deadLetters;

    public RetentionMaintenance(JdbcClient jdbc, DeadLetterRepository deadLetters) {
        this.jdbc = jdbc;
        this.deadLetters = deadLetters;
    }

    public Result run(Retention retention) {
        int expired = deadLetters.expire(retention.replayWindow());
        int deletedDeadLetters = deadLetters.deleteResolvedBefore(retention.resolvedDeadLetters());
        long deletedLedger = 0;
        int batch;
        do {
            batch = deleteLedgerBatch(retention.ledger());
            deletedLedger += batch;
        } while (batch == LEDGER_BATCH);
        int droppedPartitions = jdbc.sql("SELECT drop_expired_event_partitions(:days)")
                .param("days", Math.toIntExact(retention.events().toDays()))
                .query(Integer.class)
                .single();
        int deletedStats = jdbc.sql(
                        "DELETE FROM event_stats_minute WHERE bucket_start < now() - make_interval(secs => :seconds)")
                .param("seconds", seconds(retention.stats()))
                .update();
        deletedStats += jdbc.sql(
                        "DELETE FROM latency_stats_minute WHERE bucket_start < now() - make_interval(secs => :seconds)")
                .param("seconds", seconds(retention.stats()))
                .update();
        return new Result(expired, deletedDeadLetters, deletedLedger, droppedPartitions, deletedStats);
    }

    private int deleteLedgerBatch(Duration ledgerRetention) {
        return jdbc.sql("""
                        DELETE FROM processed_events WHERE event_id IN (
                            SELECT p.event_id FROM processed_events p
                            WHERE p.processed_at < now() - make_interval(secs => :seconds)
                              AND EXISTS (SELECT 1 FROM run_reconciliations r
                                          WHERE r.run_id = p.run_id AND r.result IN ('PASS', 'FAIL'))
                            LIMIT :batch)
                        """)
                .param("seconds", seconds(ledgerRetention))
                .param("batch", LEDGER_BATCH)
                .update();
    }

    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
