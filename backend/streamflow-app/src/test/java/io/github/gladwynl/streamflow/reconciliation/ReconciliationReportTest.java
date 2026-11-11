package io.github.gladwynl.streamflow.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.SinkFacts;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ReconciliationReportTest {

    private static final UUID RUN = UUID.randomUUID();
    private static final Instant NOW = Instant.parse("2026-11-11T12:00:00Z");

    /**
     * 1,000 attempted: 990 acked, 4 definite failures, 6 ambiguous of which 2 landed. Of the 992 that reached the
     * sink: {@code processed} in the ledger (some via replay), {@code unresolved} and {@code discarded} dead letters.
     */
    private static SinkFacts sink(
            long processed,
            long unresolved,
            long discarded,
            long inFlight,
            long missing,
            long unexpected,
            long runStats) {
        return new SinkFacts(
                processed,
                5,
                unresolved,
                discarded,
                inFlight,
                missing,
                List.of(),
                unexpected,
                2,
                0,
                runStats,
                processed,
                37);
    }

    private static ReconciliationReport evaluate(RunStatus status, boolean drained, SinkFacts sink) {
        return ReconciliationReport.evaluate(RUN, NOW, status, null, 1_000, 990, 4, 6, drained, true, sink);
    }

    @Test
    void passesWhenEveryAcknowledgedEventIsProcessedOnceOrAccountedForAsADeadLetter() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(980, 10, 2, 0, 0, 0, 980));

        assertThat(report.result()).isEqualTo(Result.PASS);
        assertThat(report.reasons()).isEmpty();
        assertThat(report.deadLetterUnresolved()).isEqualTo(10);
        assertThat(report.discarded()).isEqualTo(2);
        assertThat(report.replayedAndProcessed()).isEqualTo(5);
        assertThat(report.duplicatesSuppressed())
                .as("reported, never added to event counts")
                .isEqualTo(37);
    }

    @Test
    void failsOnMissingEvents() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(979, 10, 2, 0, 1, 0, 979));

        assertThat(report.result()).isEqualTo(Result.FAIL);
        assertThat(report.reasons())
                .contains("1 acknowledged event(s) reached neither the ledger nor the dead letters")
                .contains("processed 979 + dead-letter unresolved 10 + discarded 2 != acknowledged 990"
                        + " + ambiguous-that-landed 2");
    }

    @Test
    void failsOnUnexpectedEventsAndDoubleEffects() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(980, 10, 2, 0, 0, 1, 981));

        assertThat(report.result()).isEqualTo(Result.FAIL);
        assertThat(report.reasons())
                .containsExactly(
                        "1 sink event(s) were never attempted or definitely not published",
                        "effects applied 981 times for 980 distinct events");
    }

    @Test
    void anOpenReplayKeepsTheRunUndecided() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(979, 10, 2, 1, 0, 0, 979));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).containsExactly("1 replay(s) are still in flight");
    }

    @Test
    void isIncompleteUntilDrainedEvenIfCountsAlreadyMatch() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, false, sink(980, 10, 2, 0, 0, 0, 980));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).containsExactly("consumers have not caught up (lag is not zero)");
    }

    @Test
    void aRunThatNeverFinishedCanNeverPass() {
        ReconciliationReport report = evaluate(RunStatus.RUNNING, true, sink(980, 10, 2, 0, 0, 0, 980));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).first().asString().contains("still RUNNING");
    }

    @Test
    void producerOutcomesMustCoverEveryAttempt() {
        ReconciliationReport report = ReconciliationReport.evaluate(
                RUN, NOW, RunStatus.COMPLETED, null, 1_000, 989, 4, 6, true, true, sink(979, 10, 2, 0, 0, 0, 979));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).containsExactly("producer outcomes do not cover every attempt");
    }

    @Test
    void eventRowsAreNotComparedOnceTheRunMayHaveAgedOut() {
        SinkFacts aged = new SinkFacts(980, 0, 10, 2, 0, 0, List.of(), 0, 2, 0, 980, 0, 0);

        ReconciliationReport report =
                ReconciliationReport.evaluate(RUN, NOW, RunStatus.COMPLETED, null, 1_000, 990, 4, 6, true, false, aged);

        assertThat(report.result()).isEqualTo(Result.PASS);
        assertThat(report.eventRowsChecked()).isFalse();
    }
}
