package io.github.gladwynl.streamflow.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Sink-side facts about a run, read from the ledger, the events table and the run statistics. */
@Repository
public class ReconciliationQueries {

    private static final int SAMPLE_SIZE = 20;

    private final JdbcClient jdbc;

    public ReconciliationQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Distinct events of the run in the ledger. */
    public long processed(UUID runId) {
        return count("SELECT count(*) FROM processed_events WHERE run_id = :runId", runId);
    }

    /** Attempted sequence numbers that neither failed nor reached the ledger. */
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

    /** Ledger rows that should not exist: never attempted, or recorded as definitely not published. */
    public long unexpected(UUID runId, long attempted) {
        return jdbc.sql("""
                        SELECT count(*) FROM processed_events p
                        WHERE p.run_id = :runId
                          AND (p.run_seq < 0 OR p.run_seq >= :attempted
                               OR EXISTS (SELECT 1 FROM publish_failures f
                                          WHERE f.run_id = p.run_id AND f.sequence = p.run_seq
                                            AND f.outcome = 'DEFINITE'))
                        """)
                .param("runId", runId)
                .param("attempted", attempted)
                .query(Long.class)
                .single();
    }

    /** Ambiguous publications that turned out to reach the ledger. */
    public long ambiguousLanded(UUID runId) {
        return count("""
                SELECT count(*) FROM publish_failures f
                WHERE f.run_id = :runId AND f.outcome = 'AMBIGUOUS'
                  AND EXISTS (SELECT 1 FROM processed_events p WHERE p.run_id = f.run_id AND p.run_seq = f.sequence)
                """, runId);
    }

    /** Sequence numbers carried by more than one distinct event id. */
    public long reusedSequences(UUID runId) {
        return count("""
                SELECT count(*) FROM (
                    SELECT run_seq FROM processed_events WHERE run_id = :runId GROUP BY run_seq HAVING count(*) > 1
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
            %s
            """;
}
