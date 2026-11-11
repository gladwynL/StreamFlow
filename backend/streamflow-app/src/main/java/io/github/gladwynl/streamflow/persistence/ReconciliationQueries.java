package io.github.gladwynl.streamflow.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Sink-side facts about a run, read from the ledger, the dead letters, the events table and the run statistics.
 *
 * <p>Each attempted sequence number has at most one sink state. A sequence in the ledger is processed, whatever its
 * dead-letter history (for example, dead-lettered and then successfully replayed). Otherwise the <em>latest</em>
 * dead letter of the sequence decides: PENDING or EXPIRED is unresolved, DISCARDED is discarded, REPLAY_REQUESTED or
 * REPLAYED (but not yet in the ledger) is a replay in flight.
 */
@Repository
public class ReconciliationQueries {

    private static final int SAMPLE_SIZE = 20;

    /** Sink states of dead-lettered sequences that are not in the ledger. */
    public record DeadLetterStates(long unresolved, long discarded, long replayInFlight) {}

    private final JdbcClient jdbc;

    public ReconciliationQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Distinct events of the run in the ledger. */
    public long processed(UUID runId) {
        return count("SELECT count(*) FROM processed_events WHERE run_id = :runId", runId);
    }

    public DeadLetterStates deadLetterStates(UUID runId) {
        return jdbc.sql("""
                        WITH latest AS (
                            SELECT DISTINCT ON (d.run_seq) d.run_seq, d.status
                            FROM dead_letter_events d
                            WHERE d.run_id = :runId AND d.run_seq IS NOT NULL
                              AND NOT EXISTS (SELECT 1 FROM processed_events p
                                              WHERE p.run_id = d.run_id AND p.run_seq = d.run_seq)
                            ORDER BY d.run_seq, d.id DESC
                        )
                        SELECT count(*) FILTER (WHERE status IN ('PENDING', 'EXPIRED')) AS unresolved,
                               count(*) FILTER (WHERE status = 'DISCARDED') AS discarded,
                               count(*) FILTER (WHERE status IN ('REPLAY_REQUESTED', 'REPLAYED')) AS in_flight
                        FROM latest
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> new DeadLetterStates(
                        rs.getLong("unresolved"), rs.getLong("discarded"), rs.getLong("in_flight")))
                .single();
    }

    /** Processed events that reached the ledger through a replay of a dead letter. */
    public long replayedAndProcessed(UUID runId) {
        return count("""
                SELECT count(DISTINCT d.run_seq) FROM dead_letter_events d
                WHERE d.run_id = :runId AND d.status = 'REPLAYED'
                  AND EXISTS (SELECT 1 FROM processed_events p WHERE p.run_id = d.run_id AND p.run_seq = d.run_seq)
                """, runId);
    }

    /** Attempted sequence numbers with no publish failure, no ledger row and no dead letter. */
    public long missing(UUID runId, long attempted) {
        return jdbc.sql(MISSING_SQL.formatted("count(*)", ""))
                .param("runId", runId)
                .param("lastSeq", attempted - 1)
                .query(Long.class)
                .single();
    }

    public List<Long> missingSample(UUID runId, long attempted) {
        return jdbc.sql(MISSING_SQL.formatted("s", "ORDER BY s LIMIT " + SAMPLE_SIZE))
                .param("runId", runId)
                .param("lastSeq", attempted - 1)
                .query(Long.class)
                .list();
    }

    /** Sequences in the sink that should not exist: never attempted, or recorded as definitely not published. */
    public long unexpected(UUID runId, long attempted) {
        return jdbc.sql("""
                        WITH sink AS (
                            SELECT run_seq FROM processed_events WHERE run_id = :runId
                            UNION
                            SELECT run_seq FROM dead_letter_events WHERE run_id = :runId AND run_seq IS NOT NULL
                        )
                        SELECT count(*) FROM sink s
                        WHERE s.run_seq < 0 OR s.run_seq >= :attempted
                           OR EXISTS (SELECT 1 FROM publish_failures f
                                      WHERE f.run_id = :runId AND f.sequence = s.run_seq AND f.outcome = 'DEFINITE')
                        """)
                .param("runId", runId)
                .param("attempted", attempted)
                .query(Long.class)
                .single();
    }

    /** Ambiguous publications that turned out to reach the sink (ledger or dead letters). */
    public long ambiguousLanded(UUID runId) {
        return count("""
                SELECT count(*) FROM publish_failures f
                WHERE f.run_id = :runId AND f.outcome = 'AMBIGUOUS'
                  AND (EXISTS (SELECT 1 FROM processed_events p WHERE p.run_id = f.run_id AND p.run_seq = f.sequence)
                       OR EXISTS (SELECT 1 FROM dead_letter_events d
                                  WHERE d.run_id = f.run_id AND d.run_seq = f.sequence))
                """, runId);
    }

    /** Sequence numbers carried by more than one distinct event id anywhere in the sink. */
    public long reusedSequences(UUID runId) {
        return count("""
                SELECT count(*) FROM (
                    SELECT run_seq FROM (
                        SELECT run_seq, event_id FROM processed_events WHERE run_id = :runId
                        UNION
                        SELECT run_seq, event_id FROM dead_letter_events
                        WHERE run_id = :runId AND run_seq IS NOT NULL AND event_id IS NOT NULL
                    ) pairs
                    GROUP BY run_seq HAVING count(*) > 1
                ) reused
                """, runId);
    }

    public long runStatsProcessed(UUID runId) {
        return count("SELECT coalesce(sum(processed), 0) FROM run_stats WHERE run_id = :runId", runId);
    }

    public long duplicatesSuppressed(UUID runId) {
        return count("SELECT coalesce(sum(duplicates_suppressed), 0) FROM run_stats WHERE run_id = :runId", runId);
    }

    public long eventRows(UUID runId) {
        return count("SELECT count(*) FROM events WHERE run_id = :runId", runId);
    }

    public boolean hasFinalResult(UUID runId) {
        return count(
                        "SELECT count(*) FROM run_reconciliations WHERE run_id = :runId AND result IN ('PASS', 'FAIL')",
                        runId)
                > 0;
    }

    public void saveResult(UUID runId, String result, String detailsJson) {
        jdbc.sql("""
                        INSERT INTO run_reconciliations (run_id, result, details)
                        VALUES (:runId, :result, CAST(:details AS jsonb))
                        """)
                .param("runId", runId)
                .param("result", result)
                .param("details", detailsJson)
                .update();
    }

    private long count(String sql, UUID runId) {
        return jdbc.sql(sql).param("runId", runId).query(Long.class).single();
    }

    private static final String MISSING_SQL = """
            SELECT %s FROM generate_series(0, :lastSeq) AS s
            WHERE NOT EXISTS (SELECT 1 FROM processed_events p WHERE p.run_id = :runId AND p.run_seq = s)
              AND NOT EXISTS (SELECT 1 FROM publish_failures f WHERE f.run_id = :runId AND f.sequence = s)
              AND NOT EXISTS (SELECT 1 FROM dead_letter_events d WHERE d.run_id = :runId AND d.run_seq = s)
            %s
            """;
}
