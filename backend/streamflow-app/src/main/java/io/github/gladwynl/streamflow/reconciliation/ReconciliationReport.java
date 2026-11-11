package io.github.gladwynl.streamflow.reconciliation;

import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The result of reconciling one run (ADR-0009). Producer facts come from the run's accounting, sink facts from the
 * ledger and the run statistics. Each event is counted once: the ledger holds distinct event ids, and duplicate
 * deliveries are reported separately as {@code duplicatesSuppressed}.
 *
 * <p>Phase 2 has no dead-letter topic, so every acknowledged event must reach the ledger; Phase 3 adds dead-lettered
 * and discarded events as accounted states.
 */
public record ReconciliationReport(
        UUID runId,
        Result result,
        List<String> reasons,
        Instant checkedAt,
        RunStatus runStatus,
        boolean drained,
        long attempted,
        long acked,
        long failedDefinite,
        long failedAmbiguous,
        long ambiguousLanded,
        long processed,
        long missing,
        List<Long> missingSample,
        long unexpected,
        long reusedSequences,
        long runStatsProcessed,
        long eventRows,
        long duplicatesSuppressed) {

    public enum Result {
        /** Drained, and every check passed. */
        PASS,
        /** Drained, and at least one check failed: an event was lost, unexpected or applied twice. */
        FAIL,
        /** Not decidable yet: the run did not finish cleanly, or consumers have not caught up. */
        INCOMPLETE
    }

    /** The sink-side counts read from PostgreSQL. */
    public record SinkFacts(
            long processed,
            long missing,
            List<Long> missingSample,
            long unexpected,
            long ambiguousLanded,
            long reusedSequences,
            long runStatsProcessed,
            long eventRows,
            long duplicatesSuppressed) {}

    /** Applies the checks of Phase 0 §4.7 to the facts of a run. */
    static ReconciliationReport evaluate(
            UUID runId,
            Instant checkedAt,
            RunStatus runStatus,
            String runDetail,
            long attempted,
            long acked,
            long failedDefinite,
            long failedAmbiguous,
            boolean drained,
            SinkFacts sink) {
        List<String> incomplete = new ArrayList<>();
        if (runStatus == RunStatus.RUNNING) {
            incomplete.add("the run is still RUNNING: the generator has not finished, or it crashed");
        } else if (runStatus == RunStatus.INCOMPLETE) {
            incomplete.add("the run is INCOMPLETE: " + runDetail);
        }
        if (attempted != acked + failedDefinite + failedAmbiguous) {
            incomplete.add("producer outcomes do not cover every attempt");
        }
        if (!drained) {
            incomplete.add("consumers have not caught up (lag is not zero)");
        }

        List<String> failures = new ArrayList<>();
        if (sink.missing() > 0) {
            failures.add(sink.missing() + " acknowledged event(s) never reached the ledger");
        }
        if (sink.unexpected() > 0) {
            failures.add(sink.unexpected() + " ledger event(s) were never attempted or definitely not published");
        }
        if (sink.reusedSequences() > 0) {
            failures.add(sink.reusedSequences() + " sequence number(s) carried more than one event id");
        }
        if (sink.processed() != acked + sink.ambiguousLanded()) {
            failures.add("processed %d != acknowledged %d + ambiguous-that-landed %d"
                    .formatted(sink.processed(), acked, sink.ambiguousLanded()));
        }
        if (sink.runStatsProcessed() != sink.processed()) {
            failures.add("effects applied %d times for %d distinct events"
                    .formatted(sink.runStatsProcessed(), sink.processed()));
        }
        if (sink.eventRows() != sink.processed()) {
            failures.add("%d event rows for %d distinct events".formatted(sink.eventRows(), sink.processed()));
        }

        Result result;
        List<String> reasons;
        if (!incomplete.isEmpty()) {
            result = Result.INCOMPLETE;
            reasons = incomplete;
        } else if (!failures.isEmpty()) {
            result = Result.FAIL;
            reasons = failures;
        } else {
            result = Result.PASS;
            reasons = List.of();
        }
        return new ReconciliationReport(
                runId,
                result,
                List.copyOf(reasons),
                checkedAt,
                runStatus,
                drained,
                attempted,
                acked,
                failedDefinite,
                failedAmbiguous,
                sink.ambiguousLanded(),
                sink.processed(),
                sink.missing(),
                List.copyOf(sink.missingSample()),
                sink.unexpected(),
                sink.reusedSequences(),
                sink.runStatsProcessed(),
                sink.eventRows(),
                sink.duplicatesSuppressed());
    }
}
