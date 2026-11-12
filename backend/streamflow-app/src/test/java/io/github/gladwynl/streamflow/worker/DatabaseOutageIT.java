package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * PostgreSQL becomes unavailable in the middle of a run ({@code docker pause}). Infrastructure failures must pause
 * processing and retry instead of dead-lettering, the consumer must not be kicked out of its group while it backs
 * off, and processing must resume by itself when the database returns. Verifies Phase 0 §9 item 5.
 */
class DatabaseOutageIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("outage"));
    private static final int EVENTS = 20_000;

    @Test
    void anOutagePausesProcessingWithoutDeadLettersAndProcessingResumesAfterRecovery() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            RunSummary run = tools.simulation().run(new SimulationSpec(41, EVENTS, 10_000, 50_000));
            assertThat(run.acked()).isEqualTo(EVENTS);

            // A short socket timeout turns a frozen database into connection errors within seconds.
            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(
                    PIPELINE, "--spring.datasource.hikari.data-source-properties.socketTimeout=3")) {
                WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);
                // The consumers' initial joins rebalance the group; measure only after every consumer thread owns
                // its partitions, so any rebalance counted below happened during the outage.
                DeadLetterIT.awaitAssignment(worker);
                double rebalancesBefore = metrics.rebalances();
                // Pause as soon as processing is under way, so most of the run is still ahead in either processing
                // mode (batch mode drains thousands of events per second).
                await().atMost(Duration.ofSeconds(60))
                        .pollInterval(Duration.ofMillis(20))
                        .until(() -> metrics.processed.count() >= 1);

                IntegrationTestInfrastructure.pausePostgres();
                try {
                    // Observable outage: every consumer thread fails with infrastructure errors and backs off.
                    await().atMost(Duration.ofSeconds(60))
                            .until(() -> metrics.inOutage() && metrics.failures(FailureCategory.INFRASTRUCTURE) >= 6);
                    assertThat(tools.deadLetterTopicRecords())
                            .as("no dead letters during the outage")
                            .isZero();
                } finally {
                    IntegrationTestInfrastructure.unpausePostgres();
                }

                ReconciliationReport report =
                        tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(180));

                assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
                assertThat(report.processed()).isEqualTo(EVENTS);
                assertThat(report.deadLetterUnresolved()).isZero();
                assertThat(tools.deadLetterTopicRecords()).isZero();
                assertThat(metrics.deadLetteredCount(FailureCategory.TRANSIENT)
                                + metrics.deadLetteredCount(FailureCategory.BAD_RECORD))
                        .isZero();
                assertThat(metrics.inOutage()).isFalse();
                assertThat(metrics.recoveries()).isPositive();
                assertThat(metrics.rebalances())
                        .as("backing off must not exceed max.poll.interval.ms and trigger a rebalance")
                        .isEqualTo(rebalancesBefore);
                System.out.printf(
                        "OBSERVED outage: %.0f infrastructure failures, %.0f recoveries, 0 dead letters,"
                                + " %.0f rebalances during the outage, %d processed, %d duplicate deliveries%n",
                        metrics.failures(FailureCategory.INFRASTRUCTURE),
                        metrics.recoveries(),
                        metrics.rebalances() - rebalancesBefore,
                        report.processed(),
                        report.duplicatesSuppressed());
            }
        }
    }
}
