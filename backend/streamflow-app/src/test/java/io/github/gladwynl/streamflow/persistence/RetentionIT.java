package io.github.gladwynl.streamflow.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.config.StreamflowProperties.Retention;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.Status;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Requirement 12: retention never deletes deduplication records inside the redelivery and replay window, never
 * deletes the ledger of a run that has not been finally reconciled, and never deletes unresolved dead letters.
 */
class RetentionIT {

    private static final Retention DEFAULTS = new Retention(
            Duration.ofDays(8),
            Duration.ofDays(7),
            Duration.ofDays(1),
            Duration.ofDays(7),
            Duration.ofDays(30),
            Duration.ofDays(14));

    private final JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();
    private final DeadLetterRepository deadLetters = new DeadLetterRepository(jdbc);
    private final RetentionMaintenance retention = new RetentionMaintenance(jdbc, deadLetters);

    @Test
    void retentionRespectsTheDeduplicationWindowOpenRunsAndUnresolvedDeadLetters() {
        UUID reconciledRun = run("PASS");
        UUID openRun = run(null);
        UUID oldReconciled = ledger(reconciledRun, 1, "9 days");
        UUID recentReconciled = ledger(reconciledRun, 2, "7 days 23 hours");
        UUID oldOpen = ledger(openRun, 1, "9 days");

        long pendingOld = deadLetter(1, "PENDING", "8 days", "8 days");
        long pendingRecent = deadLetter(2, "PENDING", "6 days", "6 days");
        long replayedOld = deadLetter(3, "REPLAYED", "20 days", "15 days");
        long discardedRecent = deadLetter(4, "DISCARDED", "20 days", "13 days");
        long expiredOld = deadLetter(5, "EXPIRED", "60 days", "50 days");

        jdbc.sql("SELECT ensure_event_partitions(10, 0)").query(Integer.class).single();
        String today = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);
        String sevenDaysAgo = LocalDate.now(ZoneOffset.UTC).minusDays(7).format(DateTimeFormatter.BASIC_ISO_DATE);
        String tenDaysAgo = LocalDate.now(ZoneOffset.UTC).minusDays(10).format(DateTimeFormatter.BASIC_ISO_DATE);
        assertThat(partitionExists(tenDaysAgo)).isTrue();

        jdbc.sql("INSERT INTO event_stats_minute VALUES (date_trunc('minute', now() - interval '31 days'),"
                        + " 'USER_LOGIN', 99, 1, 0), (date_trunc('minute', now() - interval '29 days'),"
                        + " 'USER_LOGIN', 99, 1, 0)")
                .update();

        RetentionMaintenance.Result result = retention.run(DEFAULTS);

        // Ledger: only the old row of a finally reconciled run is deleted.
        assertThat(ledgerExists(oldReconciled)).isFalse();
        assertThat(ledgerExists(recentReconciled))
                .as("inside the 8-day deduplication window")
                .isTrue();
        assertThat(ledgerExists(oldOpen))
                .as("its run has no final reconciliation yet")
                .isTrue();
        assertThat(result.deletedLedgerRows()).isPositive();

        // Dead letters: pending past the replay window expire but are kept; resolved ones go after 14 days.
        assertThat(deadLetters.find(pendingOld).orElseThrow().status()).isEqualTo(Status.EXPIRED);
        assertThat(deadLetters.find(pendingRecent).orElseThrow().status()).isEqualTo(Status.PENDING);
        assertThat(deadLetters.find(replayedOld)).isEmpty();
        assertThat(deadLetters.find(discardedRecent)).isPresent();
        assertThat(deadLetters.find(expiredOld))
                .as("unresolved evidence is never deleted")
                .isPresent();

        // Events partitions older than 7 days are dropped; today's and the 7-day-old one stay.
        assertThat(partitionExists(tenDaysAgo)).isFalse();
        assertThat(partitionExists(sevenDaysAgo)).isTrue();
        assertThat(partitionExists(today)).isTrue();
        assertThat(result.droppedEventPartitions()).isGreaterThanOrEqualTo(2);

        assertThat(jdbc.sql("SELECT count(*) FROM event_stats_minute WHERE kafka_partition = 99")
                        .query(Long.class)
                        .single())
                .isEqualTo(1);
    }

    private UUID run(String reconciliation) {
        UUID runId = UUID.randomUUID();
        jdbc.sql("INSERT INTO simulation_runs (run_id, seed, content_version, requested_events, rate_per_second,"
                        + " users, topic, status) VALUES (:id, 1, 1, 1, 1, 1, 'it.retention', 'COMPLETED')")
                .param("id", runId)
                .update();
        if (reconciliation != null) {
            jdbc.sql("INSERT INTO run_reconciliations (run_id, result, details) VALUES (:id, :result, '{}')")
                    .param("id", runId)
                    .param("result", reconciliation)
                    .update();
        }
        return runId;
    }

    private UUID ledger(UUID runId, long seq, String age) {
        UUID eventId = UUID.randomUUID();
        jdbc.sql("INSERT INTO processed_events (event_id, run_id, run_seq, kafka_partition, kafka_offset,"
                        + " processed_at) VALUES (:e, :r, :s, 0, :s, now() - CAST(:age AS interval))")
                .param("e", eventId)
                .param("r", runId)
                .param("s", seq)
                .param("age", age)
                .update();
        return eventId;
    }

    private long deadLetter(long offset, String status, String deadLetteredAge, String statusAge) {
        return jdbc.sql("INSERT INTO dead_letter_events (source_topic, source_partition, source_offset, dlt_partition,"
                        + " dlt_offset, headers, failure_category, dead_lettered_at, status, status_changed_at)"
                        + " VALUES ('it.retention.' || :tag, 0, :offset, 0, :offset, '{}', 'BAD_RECORD',"
                        + " now() - CAST(:dlAge AS interval), :status, now() - CAST(:statusAge AS interval))"
                        + " RETURNING id")
                .param("tag", UUID.randomUUID().toString())
                .param("offset", offset)
                .param("dlAge", deadLetteredAge)
                .param("status", status)
                .param("statusAge", statusAge)
                .query(Long.class)
                .single();
    }

    private boolean ledgerExists(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = :e")
                        .param("e", eventId)
                        .query(Long.class)
                        .single()
                == 1;
    }

    private boolean partitionExists(String yyyymmdd) {
        return jdbc.sql("SELECT to_regclass(:name) IS NOT NULL")
                .param("name", "events_" + yyyymmdd)
                .query(Boolean.class)
                .single();
    }
}
