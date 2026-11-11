package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventType;
import io.github.gladwynl.streamflow.persistence.FaultScenarioRepository.Kind;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Transient failures, injected deterministically by delivery attempt (default policy: 3 attempts in total):
 *
 * <ul>
 *   <li>PRODUCT_VIEWED fails on attempts 1 and 2 and succeeds on attempt 3: processed, never dead-lettered;
 *   <li>CART_UPDATED fails on attempts 1–5: retries are exhausted after attempt 3 and it is dead-lettered as
 *       TRANSIENT with {@code attempts = 3}.
 * </ul>
 */
class RetryIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("retry"));

    @Test
    void transientFailuresAreRetriedThenEitherSucceedOrAreDeadLetteredWhenExhausted() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            tools.faultScenarios().activate(Kind.TRANSIENT_FAILURES, "PRODUCT_VIEWED", 2, Duration.ofMinutes(5));
            tools.faultScenarios().activate(Kind.TRANSIENT_FAILURES, "CART_UPDATED", 5, Duration.ofMinutes(5));
            try {
                RunSummary run = tools.simulation().run(new SimulationSpec(23, 300, 5_000, 200));
                ReconciliationReport report;
                WorkerMetrics metrics;
                try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(
                        PIPELINE, "--streamflow.fault-injection.enabled=true")) {
                    report = tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(120));
                    metrics = worker.getBean(WorkerMetrics.class);
                }
                Map<String, Long> byType = eventTypes(tools, run.runId());
                long views = byType.getOrDefault(EventType.PRODUCT_VIEWED.name(), 0L);
                long carts = deadLettersOfType(tools, run.runId(), EventType.CART_UPDATED);

                assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
                assertThat(views)
                        .as("product views all processed after retries")
                        .isPositive();
                assertThat(byType).doesNotContainKey(EventType.CART_UPDATED.name());
                assertThat(carts).isPositive();
                assertThat(report.deadLetterUnresolved()).isEqualTo(carts);
                assertThat(report.processed() + report.deadLetterUnresolved()).isEqualTo(300);

                assertThat(tools.jdbc()
                                .sql("SELECT DISTINCT failure_category || '/' || attempts FROM dead_letter_events"
                                        + " WHERE run_id = :runId")
                                .param("runId", run.runId())
                                .query(String.class)
                                .list())
                        .containsExactly("TRANSIENT/3");
                // Two failed attempts per product view, three per cart update; nothing else failed.
                assertThat(metrics.failures(FailureCategory.TRANSIENT)).isEqualTo(2.0 * views + 3.0 * carts);
                assertThat(metrics.deadLetteredCount(FailureCategory.TRANSIENT)).isEqualTo(carts);
                assertThat(metrics.failures(FailureCategory.INFRASTRUCTURE)).isZero();
            } finally {
                tools.faultScenarios().deactivateAll();
            }
        }
    }

    private static Map<String, Long> eventTypes(PipelineTools tools, UUID runId) {
        return tools
                .jdbc()
                .sql("SELECT event_type, count(*) AS n FROM events WHERE run_id = :runId GROUP BY event_type")
                .param("runId", runId)
                .query((rs, rowNum) -> Map.entry(rs.getString("event_type"), rs.getLong("n")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static long deadLettersOfType(PipelineTools tools, UUID runId, EventType type) {
        return tools.jdbc()
                .sql("SELECT count(*) FROM dead_letter_events WHERE run_id = :runId"
                        + " AND convert_from(payload, 'UTF8')::jsonb ->> 'eventType' = :type")
                .param("runId", runId)
                .param("type", type.name())
                .query(Long.class)
                .single();
    }
}
