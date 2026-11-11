package io.github.gladwynl.streamflow.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.persistence.RunRepository.FailureOutcome;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Requirement 13, producer side: runs are published by hand so that producer outcomes can be controlled exactly.
 * (The dead-letter states — unresolved, replayed, discarded — are covered with a real run in {@code ReplayIT}.)
 */
class ReconciliationIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("reconcile"));
    private static final int EVENTS = 100;

    /**
     * Seq 7: ambiguous, never reached Kafka. Seq 8: ambiguous, did reach Kafka. Seq 9: definite failure, not sent.
     * Everything else acknowledged. Expected PASS: 98 processed = 97 acknowledged + 1 ambiguous-that-landed.
     */
    @Test
    void ambiguousAndDefiniteOutcomesAreAccountedForWithoutDoubleCounting() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE);
                ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            UUID runId = publish(tools, Set.of(7L, 9L));
            tools.runs().recordFailure(runId, 7, UUID.randomUUID(), FailureOutcome.AMBIGUOUS, "TimeoutException");
            tools.runs().recordFailure(runId, 8, UUID.randomUUID(), FailureOutcome.AMBIGUOUS, "TimeoutException");
            tools.runs().recordFailure(runId, 9, UUID.randomUUID(), FailureOutcome.DEFINITE, "SerializationException");
            tools.runs().finish(runId, RunStatus.COMPLETED, null, EVENTS, EVENTS - 3, 1, 2);

            ReconciliationReport report = tools.reconciliation().awaitDrainAndReconcile(runId, Duration.ofSeconds(60));

            assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
            assertThat(report.processed()).isEqualTo(98);
            assertThat(report.ambiguousLanded()).isEqualTo(1);
            assertThat(report.missing()).isZero();
            assertThat(report.unexpected()).isZero();
        }
    }

    /** An event recorded as definitely not published shows up in the sink: reported as unexpected. */
    @Test
    void aDefiniteFailureThatReachedTheSinkFailsReconciliation() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE);
                ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            UUID runId = publish(tools, Set.of());
            tools.runs().recordFailure(runId, 3, UUID.randomUUID(), FailureOutcome.DEFINITE, "misclassified");
            tools.runs().finish(runId, RunStatus.COMPLETED, null, EVENTS, EVENTS - 1, 1, 0);

            ReconciliationReport report = tools.reconciliation().awaitDrainAndReconcile(runId, Duration.ofSeconds(60));

            assertThat(report.result()).isEqualTo(Result.FAIL);
            assertThat(report.unexpected()).isEqualTo(1);
        }
    }

    /** An acknowledged event that never reached the sink: reported as missing, with its sequence number. */
    @Test
    void anAcknowledgedEventThatNeverArrivedFailsReconciliation() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE);
                ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            UUID runId = publish(tools, Set.of(42L));
            tools.runs().finish(runId, RunStatus.COMPLETED, null, EVENTS, EVENTS, 0, 0);

            ReconciliationReport report = tools.reconciliation().awaitDrainAndReconcile(runId, Duration.ofSeconds(60));

            assertThat(report.result()).isEqualTo(Result.FAIL);
            assertThat(report.missing()).isEqualTo(1);
            assertThat(report.missingSample()).containsExactly(42L);
        }
    }

    /** Creates a run row and publishes its events except {@code skipped} sequence numbers. */
    private static UUID publish(PipelineTools tools, Set<Long> skipped) throws Exception {
        UUID runId = UUID.randomUUID();
        tools.runs().create(runId, 3, 1, EVENTS, 1_000, 500, PIPELINE.eventsTopic());
        List<EventEnvelope> events = TestRecords.events(runId, 3, EVENTS);
        for (EventEnvelope event : events) {
            if (!skipped.contains(event.sequence())) {
                tools.producer()
                        .send(TestRecords.record(PIPELINE.eventsTopic(), event))
                        .get(30, TimeUnit.SECONDS);
            }
        }
        return runId;
    }
}
