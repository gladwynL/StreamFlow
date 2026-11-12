package io.github.gladwynl.streamflow.loadgen;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Read-only measurement queries of the benchmark harness. They run against the dedicated benchmark database after a
 * run has drained, so they may scan the run's rows; none of them is used by the application itself.
 */
class BenchmarkQueries {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkQueries.class);

    private final JdbcClient jdbc;

    BenchmarkQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Distinct events of the run applied so far, and duplicate deliveries suppressed by the ledger. */
    long[] processedAndDuplicates(UUID runId) {
        return jdbc.sql("""
                        SELECT coalesce(sum(processed), 0) AS p, coalesce(sum(duplicates_suppressed), 0) AS d
                        FROM run_stats WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> new long[] {rs.getLong("p"), rs.getLong("d")})
                .single();
    }

    long deadLetters(UUID runId) {
        return jdbc.sql("SELECT count(*) FROM dead_letter_events WHERE run_id = :runId")
                .param("runId", runId)
                .query(Long.class)
                .single();
    }

    /**
     * Consumer throughput from the event rows themselves: distinct events divided by the span between the first and
     * the last processing transaction (database clock). Independent of the sampling interval.
     */
    Map<String, Object> consumerThroughput(UUID runId) {
        return jdbc.sql("""
                        SELECT count(*) AS events, min(ingested_at) AS first_at, max(ingested_at) AS last_at,
                               extract(epoch FROM max(ingested_at) - min(ingested_at)) AS span_seconds
                        FROM events WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    long events = rs.getLong("events");
                    double span = rs.getDouble("span_seconds");
                    m.put("events", events);
                    m.put("firstProcessedAt", instant(rs.getObject("first_at", OffsetDateTime.class)));
                    m.put("lastProcessedAt", instant(rs.getObject("last_at", OffsetDateTime.class)));
                    m.put("spanSeconds", span);
                    m.put("eventsPerSecond", span > 0 ? (events - 1) / span : null);
                    return m;
                })
                .single();
    }

    /**
     * Exact processing latency of the run's events (Kafka append to start of the processing transaction), in
     * milliseconds. Unlike the dashboard histogram these are exact percentiles, not bucket bounds.
     */
    Map<String, Object> latency(UUID runId) {
        return jdbc.sql("""
                        WITH l AS (SELECT extract(epoch FROM ingested_at - kafka_ts) * 1000 AS ms
                                   FROM events WHERE run_id = :runId)
                        SELECT count(*) AS n,
                               percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) AS p50,
                               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
                               percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99,
                               max(ms) AS max
                        FROM l
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("samples", rs.getLong("n"));
                    m.put("p50Millis", rs.getObject("p50"));
                    m.put("p95Millis", rs.getObject("p95"));
                    m.put("p99Millis", rs.getObject("p99"));
                    m.put("maxMillis", rs.getObject("max"));
                    return m;
                })
                .single();
    }

    /** Events per partition and the time each partition spent processing them (skew). */
    List<Map<String, Object>> partitions(UUID runId) {
        return jdbc.sql("""
                        SELECT kafka_partition, count(*) AS events,
                               extract(epoch FROM max(ingested_at) - min(ingested_at)) AS span_seconds
                        FROM events WHERE run_id = :runId GROUP BY kafka_partition ORDER BY kafka_partition
                        """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("partition", rs.getInt("kafka_partition"));
                    m.put("events", rs.getLong("events"));
                    m.put("spanSeconds", rs.getDouble("span_seconds"));
                    return m;
                })
                .list();
    }

    /** The statements that took the most database time (requires pg_stat_statements in the benchmark database). */
    List<Map<String, Object>> topStatements(int limit) {
        return optional(
                "pg_stat_statements",
                () -> jdbc.sql("""
                        SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 220) AS query, calls,
                               round(total_exec_time::numeric, 1) AS total_ms,
                               round(mean_exec_time::numeric, 4) AS mean_ms, rows,
                               shared_blks_hit, shared_blks_read, wal_records, wal_bytes
                        FROM pg_stat_statements
                        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                        ORDER BY total_exec_time DESC LIMIT :limit
                        """)
                        .param("limit", limit)
                        .query((rs, rowNum) -> row(rs))
                        .list());
    }

    /** Transactions, rows and block statistics of the benchmark database since the last reset. */
    Map<String, Object> database() {
        return optional(
                "pg_stat_database",
                () -> jdbc.sql("""
                        SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, blks_read, blks_hit,
                               deadlocks, temp_bytes, active_time, session_time
                        FROM pg_stat_database WHERE datname = current_database()
                        """).query((rs, rowNum) -> row(rs)).single());
    }

    /** WAL writes, flushes and their time (PostgreSQL 18 pg_stat_io). */
    List<Map<String, Object>> walIo() {
        return optional(
                "pg_stat_io", () -> jdbc.sql("""
                        SELECT backend_type, object, context, writes, write_bytes, write_time, fsyncs, fsync_time
                        FROM pg_stat_io WHERE object = 'wal' AND (writes > 0 OR fsyncs > 0)
                        """).query((rs, rowNum) -> row(rs)).list());
    }

    Map<String, Object> settings() {
        Map<String, Object> m = new LinkedHashMap<>();
        jdbc.sql("""
                        SELECT name, setting FROM pg_settings
                        WHERE name IN ('server_version', 'synchronous_commit', 'fsync', 'wal_sync_method',
                                       'shared_buffers', 'max_connections', 'wal_level', 'commit_delay',
                                       'track_io_timing', 'track_wal_io_timing', 'max_wal_size', 'full_page_writes')
                        ORDER BY name
                        """)
                .query((rs, rowNum) -> m.put(rs.getString("name"), rs.getString("setting")))
                .list();
        return m;
    }

    Map<String, Object> tableSizes() {
        return jdbc.sql("""
                        SELECT pg_total_relation_size('processed_events') AS ledger_bytes,
                               (SELECT coalesce(sum(pg_total_relation_size(inhrelid)), 0) FROM pg_inherits
                                WHERE inhparent = 'events'::regclass) AS events_bytes
                        """).query((rs, rowNum) -> row(rs)).single();
    }

    private static Map<String, Object> row(java.sql.ResultSet rs) throws java.sql.SQLException {
        Map<String, Object> m = new LinkedHashMap<>();
        java.sql.ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Object value = rs.getObject(i);
            m.put(meta.getColumnLabel(i), value instanceof Number || value == null ? value : value.toString());
        }
        return m;
    }

    private static String instant(OffsetDateTime time) {
        return time == null ? null : time.toInstant().toString();
    }

    private static <T> T optional(String what, java.util.function.Supplier<T> query) {
        try {
            return query.get();
        } catch (RuntimeException e) {
            log.warn("Could not read {}: {}", what, e.getMessage());
            return null;
        }
    }
}
