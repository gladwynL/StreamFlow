package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Workers join and leave while a run is being published: a second worker joins (partitions are revoked from the
 * first and reassigned), then the first worker shuts down gracefully (its partitions move to the second).
 *
 * <p>Verifies Phase 0 §9 items 8 and 9: offsets of processed records are committed on revocation and on graceful
 * shutdown, so neither causes redelivery. Nothing is lost, and no event takes effect twice.
 */
class WorkerRestartIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("restart"));
    private static final int EVENTS = 30_000;

    @Test
    void workersJoiningAndLeavingGracefullyLoseNothingAndRedeliverNothing() throws Exception {
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            ConfigurableApplicationContext first = IntegrationTestInfrastructure.startWorker(PIPELINE);
            ConfigurableApplicationContext second = null;
            try {
                // About 10 seconds of publishing at 3,000 events/s.
                Future<RunSummary> run =
                        publisher.submit(() -> tools.simulation().run(new SimulationSpec(21, EVENTS, 3_000, 50_000)));

                await().atMost(Duration.ofSeconds(60)).until(() -> processed(tools) >= 3_000);
                second = IntegrationTestInfrastructure.startWorker(PIPELINE);

                await().atMost(Duration.ofSeconds(60)).until(() -> processed(tools) >= 15_000);
                first.close();

                RunSummary summary = run.get(120, TimeUnit.SECONDS);
                ReconciliationReport report =
                        tools.reconciliation().awaitDrainAndReconcile(summary.runId(), Duration.ofSeconds(120));

                assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
                assertThat(report.processed()).isEqualTo(EVENTS);
                assertThat(report.runStatsProcessed()).isEqualTo(EVENTS);
                assertThat(report.duplicatesSuppressed())
                        .as("a rebalance or graceful shutdown must not cause redelivery")
                        .isZero();
                System.out.printf(
                        "OBSERVED join + graceful leave: %d processed, %d duplicate deliveries%n",
                        report.processed(), report.duplicatesSuppressed());
            } finally {
                first.close();
                if (second != null) {
                    second.close();
                }
            }
        } finally {
            publisher.shutdownNow();
        }
    }

    private static long processed(PipelineTools tools) {
        return tools.jdbc()
                .sql("""
                        SELECT coalesce(sum(s.processed), 0) FROM run_stats s
                        JOIN simulation_runs r ON r.run_id = s.run_id WHERE r.topic = :topic
                        """)
                .param("topic", PIPELINE.eventsTopic())
                .query(Long.class)
                .single();
    }
}
