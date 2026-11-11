package io.github.gladwynl.streamflow.reconciliation;

import io.github.gladwynl.streamflow.kafka.ConsumerLagReader;
import io.github.gladwynl.streamflow.kafka.TopicSpec;
import io.github.gladwynl.streamflow.persistence.ReconciliationQueries;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunRecord;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.SinkFacts;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reconciles a run once the processing group has drained the run's topic, and stores the result.
 *
 * <p>"Drained" means zero lag for the processing group: offsets are committed only after the database commits of
 * their records (ADR-0005), so zero lag implies every published record of the topic has been processed.
 */
public final class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    private final RunRepository runs;
    private final ReconciliationQueries queries;
    private final ConsumerLagReader lagReader;
    private final String consumerGroup;
    private final TopicSpec topic;
    private final JsonMapper json;
    private final Clock clock;

    public ReconciliationService(
            RunRepository runs,
            ReconciliationQueries queries,
            ConsumerLagReader lagReader,
            String consumerGroup,
            TopicSpec topic,
            JsonMapper json,
            Clock clock) {
        this.runs = runs;
        this.queries = queries;
        this.lagReader = lagReader;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.json = json;
        this.clock = clock;
    }

    /** Waits up to {@code timeout} for zero lag, then reconciles (INCOMPLETE if lag remains) and stores the result. */
    public ReconciliationReport awaitDrainAndReconcile(UUID runId, Duration timeout) throws InterruptedException {
        Instant deadline = clock.instant().plus(timeout);
        boolean drained = isDrained();
        while (!drained && clock.instant().isBefore(deadline)) {
            Thread.sleep(POLL_INTERVAL);
            drained = isDrained();
        }
        return reconcile(runId, drained);
    }

    public ReconciliationReport reconcile(UUID runId) {
        return reconcile(runId, isDrained());
    }

    private ReconciliationReport reconcile(UUID runId, boolean drained) {
        RunRecord run = runs.find(runId).orElseThrow(() -> new IllegalArgumentException("Unknown run " + runId));
        long attempted = run.attempted();
        SinkFacts sink = new SinkFacts(
                queries.processed(runId),
                attempted == 0 ? 0 : queries.missing(runId, attempted),
                attempted == 0 ? List.of() : queries.missingSample(runId, attempted),
                queries.unexpected(runId, attempted),
                queries.ambiguousLanded(runId),
                queries.reusedSequences(runId),
                queries.runStatsProcessed(runId),
                queries.eventRows(runId),
                queries.duplicatesSuppressed(runId));
        ReconciliationReport report = ReconciliationReport.evaluate(
                runId,
                clock.instant(),
                run.status(),
                run.statusDetail(),
                attempted,
                run.acked(),
                run.failedDefinite(),
                run.failedAmbiguous(),
                drained,
                sink);
        queries.saveResult(runId, report.result().name(), json.writeValueAsString(report));
        if (report.result() == Result.PASS) {
            log.info(
                    "Run {} reconciled: PASS ({} processed, {} duplicate deliveries suppressed)",
                    runId,
                    report.processed(),
                    report.duplicatesSuppressed());
        } else {
            log.warn("Run {} reconciled: {} {}", runId, report.result(), report.reasons());
        }
        return report;
    }

    private boolean isDrained() {
        return lagReader.totalLag(consumerGroup, topic) == 0;
    }
}
