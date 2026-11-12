package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The Kafka broker becomes unavailable ({@code docker pause}) while a worker is processing. The worker must ride it
 * out without dead-lettering anything (a broker outage is not a record failure), reconnect by itself, process events
 * published after the outage exactly once, and leave the run reconciled. Runs in both processing modes.
 */
class KafkaOutageIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("kafka-outage"));
    private static final int EVENTS = 20_000;
    private static final int AFTER_OUTAGE = 2_000;
    private static final Duration OUTAGE = Duration.ofSeconds(15);

    @Test
    void aBrokerOutageIsRiddenOutWithoutDeadLettersOrLoss() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            RunSummary run = tools.simulation().run(new SimulationSpec(43, EVENTS, 10_000, 50_000));
            assertThat(run.acked()).isEqualTo(EVENTS);

            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
                WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);
                await().atMost(Duration.ofSeconds(60))
                        .pollInterval(Duration.ofMillis(20))
                        .until(() -> metrics.processed.count() >= 1);

                IntegrationTestInfrastructure.pauseKafka();
                try {
                    Thread.sleep(OUTAGE.toMillis()); // the outage itself: nothing to wait for but time
                } finally {
                    IntegrationTestInfrastructure.unpauseKafka();
                }

                ReconciliationReport report =
                        tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(180));
                assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
                assertThat(report.processed()).isEqualTo(EVENTS);

                // The worker reconnected by itself: events published after the outage are processed once.
                UUID later = UUID.randomUUID();
                List<EventEnvelope> events = TestRecords.events(later, 44, AFTER_OUTAGE);
                for (EventEnvelope event : events) {
                    tools.producer().send(TestRecords.record(PIPELINE.eventsTopic(), event));
                }
                tools.producer().flush();
                await().atMost(Duration.ofSeconds(120))
                        .until(() -> tools.jdbc()
                                        .sql("SELECT count(*) FROM processed_events WHERE run_id = :r")
                                        .param("r", later)
                                        .query(Long.class)
                                        .single()
                                == AFTER_OUTAGE);

                assertThat(tools.deadLetterTopicRecords())
                        .as("no dead letters from a broker outage")
                        .isZero();
                assertThat(metrics.deadLetteredCount(FailureCategory.TRANSIENT)
                                + metrics.deadLetteredCount(FailureCategory.BAD_RECORD))
                        .isZero();
                assertThat(tools.totalLag()).isZero();
            }
        }
    }
}
