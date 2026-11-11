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

    /** 1,000 attempted: 990 acked, 4 definite failures, 6 ambiguous of which 2 landed. */
    private static SinkFacts sink(long processed, long missing, long unexpected, long runStats, long rows) {
        return new SinkFacts(processed, missing, List.of(), unexpected, 2, 0, runStats, rows, 37);
    }

    private static ReconciliationReport evaluate(RunStatus status, boolean drained, SinkFacts sink) {
        return ReconciliationReport.evaluate(RUN, NOW, status, null, 1_000, 990, 4, 6, drained, sink);
    }

    @Test
    void passesWhenAcknowledgedPlusLandedAmbiguousEventsAreEachProcessedOnce() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(992, 0, 0, 992, 992));

        assertThat(report.result()).isEqualTo(Result.PASS);
        assertThat(report.reasons()).isEmpty();
        assertThat(report.duplicatesSuppressed())
                .as("reported, never added to event counts")
                .isEqualTo(37);
    }

    @Test
    void failsOnMissingEvents() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(991, 1, 0, 991, 991));

        assertThat(report.result()).isEqualTo(Result.FAIL);
        assertThat(report.reasons())
                .contains("1 acknowledged event(s) never reached the ledger")
                .contains("processed 991 != acknowledged 990 + ambiguous-that-landed 2");
    }

    @Test
    void failsOnUnexpectedEventsAndDoubleEffects() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, true, sink(992, 0, 1, 993, 992));

        assertThat(report.result()).isEqualTo(Result.FAIL);
        assertThat(report.reasons())
                .containsExactly(
                        "1 ledger event(s) were never attempted or definitely not published",
                        "effects applied 993 times for 992 distinct events");
    }

    @Test
    void isIncompleteUntilDrainedEvenIfCountsAlreadyMatch() {
        ReconciliationReport report = evaluate(RunStatus.COMPLETED, false, sink(992, 0, 0, 992, 992));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).containsExactly("consumers have not caught up (lag is not zero)");
    }

    @Test
    void aRunThatNeverFinishedCanNeverPass() {
        ReconciliationReport report = evaluate(RunStatus.RUNNING, true, sink(992, 0, 0, 992, 992));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).first().asString().contains("still RUNNING");
    }

    @Test
    void producerOutcomesMustCoverEveryAttempt() {
        ReconciliationReport report = ReconciliationReport.evaluate(
                RUN, NOW, RunStatus.COMPLETED, null, 1_000, 989, 4, 6, true, sink(991, 0, 0, 991, 991));

        assertThat(report.result()).isEqualTo(Result.INCOMPLETE);
        assertThat(report.reasons()).containsExactly("producer outcomes do not cover every attempt");
    }
}
