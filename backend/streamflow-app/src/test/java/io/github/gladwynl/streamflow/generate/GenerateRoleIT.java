package io.github.gladwynl.streamflow.generate;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The full pipeline with the real roles: the generate role publishes a seeded run, a worker processes it into
 * PostgreSQL, and the generate role reconciles it after the topic drains.
 */
class GenerateRoleIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("generate"));
    private static final JdbcClient JDBC = IntegrationTestInfrastructure.jdbc();

    @Test
    void publishesProcessesAndReconcilesSeededRuns() {
        try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            Map<String, Long> statsBefore = statsByType();

            Outcome first = generate(7, 20_000, 5_000);

            assertThat(first.exitCode()).isEqualTo(GenerateRunner.EXIT_PASS);
            ReconciliationReport report = first.report();
            assertThat(report.result()).isEqualTo(Result.PASS);
            assertThat(report.attempted()).isEqualTo(20_000);
            assertThat(report.acked()).isEqualTo(20_000);
            assertThat(report.processed()).isEqualTo(20_000);
            assertThat(report.missing()).isZero();
            assertThat(report.eventRows()).isEqualTo(20_000);
            assertThat(report.runStatsProcessed()).isEqualTo(20_000);

            // The minute aggregates grew by exactly this run's events, type by type.
            Map<String, Long> statsDelta = minus(statsByType(), statsBefore);
            assertThat(statsDelta).isEqualTo(eventsByType(first.summary().runId()));
            assertThat(statsDelta.values().stream().mapToLong(Long::longValue).sum())
                    .isEqualTo(20_000);

            // A second run with the same seed has the same content but its own identities and timestamps.
            Outcome second = generate(7, 20_000, 5_000);

            assertThat(second.report().result()).isEqualTo(Result.PASS);
            assertThat(second.summary().contentHash()).isEqualTo(first.summary().contentHash());
            assertThat(sharedEventIds(first.summary().runId(), second.summary().runId()))
                    .isZero();
            assertThat(sameOccurredAt(first.summary().runId(), second.summary().runId()))
                    .isZero();
        }
    }

    private static Outcome generate(long seed, long events, int rate) {
        ConfigurableApplicationContext context = IntegrationTestInfrastructure.runGenerate(
                PIPELINE,
                "--streamflow.generate.seed=" + seed,
                "--streamflow.generate.events=" + events,
                "--streamflow.generate.rate-per-second=" + rate,
                "--streamflow.generate.drain-timeout=120s");
        GenerateRunner runner = context.getBean(GenerateRunner.class);
        RunSummary summary = runner.summary();
        ReconciliationReport report = runner.report();
        int exitCode = SpringApplication.exit(context);
        return new Outcome(summary, report, exitCode);
    }

    private static Map<String, Long> statsByType() {
        return JDBC
                .sql("SELECT event_type, sum(event_count) AS n FROM event_stats_minute GROUP BY event_type")
                .query((rs, rowNum) -> Map.entry(rs.getString("event_type"), rs.getLong("n")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Long> eventsByType(UUID runId) {
        return JDBC
                .sql("SELECT event_type, count(*) AS n FROM events WHERE run_id = :runId GROUP BY event_type")
                .param("runId", runId)
                .query((rs, rowNum) -> Map.entry(rs.getString("event_type"), rs.getLong("n")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Long> minus(Map<String, Long> after, Map<String, Long> before) {
        return after.entrySet().stream()
                .filter(e -> e.getValue() - before.getOrDefault(e.getKey(), 0L) != 0)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() - before.getOrDefault(e.getKey(), 0L)));
    }

    private static long sharedEventIds(UUID first, UUID second) {
        return JDBC.sql("""
                        SELECT count(*) FROM processed_events a JOIN processed_events b ON a.event_id = b.event_id
                        WHERE a.run_id = :first AND b.run_id = :second
                        """)
                .param("first", first)
                .param("second", second)
                .query(Long.class)
                .single();
    }

    private static long sameOccurredAt(UUID first, UUID second) {
        return JDBC.sql("""
                        SELECT count(*) FROM events a JOIN events b ON a.run_seq = b.run_seq
                        WHERE a.run_id = :first AND b.run_id = :second AND a.occurred_at = b.occurred_at
                        """)
                .param("first", first)
                .param("second", second)
                .query(Long.class)
                .single();
    }

    private record Outcome(RunSummary summary, ReconciliationReport report, int exitCode) {}
}
