package io.github.gladwynl.streamflow.reconciliation;

import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The result of reconciling one run (ADR-0009). Producer facts come from the run's accounting; sink facts from the
 * ledger, the dead letters and the run statistics. Every attempted sequence number is counted at most once:
 *
 * <ul>
 *   <li>processed (in the ledger), including events that were dead-lettered and then replayed successfully;
 *   <li>dead-letter unresolved (latest dead letter PENDING or EXPIRED) — accounted for, awaiting an operator;
 *   <li>discarded (latest dead letter DISCARDED) — accounted for, deliberately dropped by an operator;
 *   <li>replay in flight — the run cannot be decided yet.
 * </ul>
 *
 * Duplicate deliveries are reported separately and never added to event counts.
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
        long replayedAndProcessed,
        long deadLetterUnresolved,
        long discarded,
        long replayInFlight,
        long missing,
        List<Long> missingSample,
        long unexpected,
        long reusedSequences,
        long runStatsProcessed,
        long eventRows,
        boolean eventRowsChecked,
        long duplicatesSuppressed) {

    public enum Result {
        /** Drained, and every acknowledged event is accounted for exactly once. */
        PASS,
        /** Drained, and at least one check failed: an event was lost, unexpected or applied twice. */
        FAIL,
        /** Not decidable yet: the run did not finish cleanly, consumers have not caught up, or a replay is open. */
        INCOMPLETE
    }

    /** The sink-side counts read from PostgreSQL. */
    public record SinkFacts(
            long processed,
            long replayedAndProcessed,
            long deadLetterUnresolved,
            long discarded,
            long replayInFlight,
            long missing,
            List<Long> missingSample,
            long unexpected,
            long ambiguousLanded,
            long reusedSequences,
            long runStatsProcessed,
            long eventRows,
            long duplicatesSuppressed) {}

    /**
     * Applies the checks of Phase 0 §4.7 to the facts of a run.
     *
     * @param checkEventRows false once the run's events partitions may have been dropped by retention
     */
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
            boolean checkEventRows,
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
        if (sink.replayInFlight() > 0) {
            incomplete.add(sink.replayInFlight() + " replay(s) are still in flight");
        }

        List<String> failures = new ArrayList<>();
        if (sink.missing() > 0) {
            failures.add(sink.missing() + " acknowledged event(s) reached neither the ledger nor the dead letters");
        }
        if (sink.unexpected() > 0) {
            failures.add(sink.unexpected() + " sink event(s) were never attempted or definitely not published");
        }
        if (sink.reusedSequences() > 0) {
            failures.add(sink.reusedSequences() + " sequence number(s) carried more than one event id");
        }
        long accounted = sink.processed() + sink.deadLetterUnresolved() + sink.discarded();
        if (accounted != acked + sink.ambiguousLanded()) {
            failures.add("processed %d + dead-letter unresolved %d + discarded %d != acknowledged %d"
                            .formatted(sink.processed(), sink.deadLetterUnresolved(), sink.discarded(), acked)
                    + " + ambiguous-that-landed " + sink.ambiguousLanded());
        }
        if (sink.runStatsProcessed() != sink.processed()) {
            failures.add("effects applied %d times for %d distinct events"
                    .formatted(sink.runStatsProcessed(), sink.processed()));
        }
        if (checkEventRows && sink.eventRows() != sink.processed()) {
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
                sink.replayedAndProcessed(),
                sink.deadLetterUnresolved(),
                sink.discarded(),
                sink.replayInFlight(),
                sink.missing(),
                List.copyOf(sink.missingSample()),
                sink.unexpected(),
                sink.reusedSequences(),
                sink.runStatsProcessed(),
                sink.eventRows(),
                checkEventRows,
                sink.duplicatesSuppressed());
    }
}
