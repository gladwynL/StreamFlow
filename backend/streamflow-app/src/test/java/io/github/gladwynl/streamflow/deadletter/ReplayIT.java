package io.github.gladwynl.streamflow.deadletter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.deadletter.ReplayService.Outcome;
import io.github.gladwynl.streamflow.deadletter.ReplayService.Result;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.DeadLetter;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.Status;
import io.github.gladwynl.streamflow.persistence.FaultScenarioRepository.Kind;
import io.github.gladwynl.streamflow.processing.FaultInjector;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Dead letters from a simulated faulty release (every APPLICATION_ERROR event rejected), then the "fix" (scenario
 * ended) and controlled replay:
 *
 * <ul>
 *   <li>replay once → processed once; replaying a REPLAYED dead letter is rejected;
 *   <li>a second publication of the same event (claim released after a presumed crash) → still one effect;
 *   <li>two concurrent replay requests for the same dead letters → each replayed exactly once;
 *   <li>a dead letter outside the replay window is rejected, then expired by retention; discard is final;
 *   <li>the run reconciles: processed (including replayed), unresolved and discarded, each counted once.
 * </ul>
 */
class ReplayIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("replay"));
    private static final int EVENTS = 2_000;

    @Test
    void replayIsIdempotentConcurrencySafeBoundedAndReconciles() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            tools.faultScenarios().activate(Kind.REJECT_EVENT_TYPE, "APPLICATION_ERROR", 1, Duration.ofMinutes(5));
            try (ConfigurableApplicationContext worker =
                    IntegrationTestInfrastructure.startWorker(PIPELINE, "--streamflow.fault-injection.enabled=true")) {
                RunSummary run = tools.simulation().run(new SimulationSpec(29, EVENTS, 10_000, 500));
                ReconciliationReport faulty =
                        tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(120));
                assertThat(faulty.result()).as(faulty.reasons().toString()).isEqualTo(ReconciliationReport.Result.PASS);
                List<Long> ids = deadLetterIds(tools, run.runId());
                assertThat(ids).hasSizeGreaterThanOrEqualTo(14);
                assertThat(faulty.deadLetterUnresolved()).isEqualTo(ids.size());

                // The "fix": end the scenario and wait until the worker has observed it.
                tools.faultScenarios().deactivateAll();
                FaultInjector injector = worker.getBean(FaultInjector.class);
                await().atMost(Duration.ofSeconds(10))
                        .until(() -> injector.activeScenarios().isEmpty());

                ReplayService replay = tools.replay();

                // Replay once; replaying again is rejected because it is no longer PENDING.
                long first = ids.get(0);
                assertThat(replay.replay(List.of(first)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.REPLAYED);
                assertThat(replay.replay(List.of(first)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.NOT_PENDING);

                // A second publication of the same event: replayed, then the claim cycle repeated as the sweeper
                // would after a crash between publication and the REPLAYED update.
                long second = ids.get(1);
                assertThat(replay.replay(List.of(second)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.REPLAYED);
                tools.jdbc()
                        .sql(
                                "UPDATE dead_letter_events SET status = 'PENDING', status_changed_at = now() WHERE id = :id")
                        .param("id", second)
                        .update();
                assertThat(replay.replay(List.of(second)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.REPLAYED);

                // Two concurrent requests for the same ten dead letters.
                List<Long> contested = ids.subList(2, 12);
                List<Result> results = concurrently(replay, contested);
                assertThat(results).hasSize(20);
                for (long id : contested) {
                    assertThat(results)
                            .filteredOn(r -> r.id() == id)
                            .extracting(Result::outcome)
                            .containsExactlyInAnyOrder(Outcome.REPLAYED, Outcome.NOT_PENDING);
                }

                // Outside the replay window: rejected and left PENDING, then expired by retention.
                long stale = ids.get(12);
                tools.jdbc()
                        .sql(
                                "UPDATE dead_letter_events SET dead_lettered_at = now() - interval '8 days' WHERE id = :id")
                        .param("id", stale)
                        .update();
                assertThat(replay.replay(List.of(stale)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.OUTSIDE_REPLAY_WINDOW);
                assertThat(status(tools, stale)).isEqualTo(Status.PENDING);
                tools.deadLetters().expire(Duration.ofDays(7));
                assertThat(status(tools, stale)).isEqualTo(Status.EXPIRED);
                assertThat(replay.replay(List.of(stale)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.NOT_PENDING);

                // Discard is final.
                long dropped = ids.get(13);
                assertThat(tools.deadLetters().discard(dropped, "test: not worth replaying"))
                        .isTrue();
                assertThat(replay.replay(List.of(dropped)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.NOT_PENDING);

                assertThat(replay.replay(List.of(Long.MAX_VALUE)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.NOT_FOUND);

                ReconciliationReport report =
                        tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(120));

                int replayed = 2 + contested.size();
                assertThat(report.result()).as(report.reasons().toString()).isEqualTo(ReconciliationReport.Result.PASS);
                assertThat(report.replayedAndProcessed()).isEqualTo(replayed);
                assertThat(report.discarded()).isEqualTo(1);
                assertThat(report.deadLetterUnresolved()).isEqualTo(ids.size() - replayed - 1);
                assertThat(report.processed() + report.deadLetterUnresolved() + report.discarded())
                        .isEqualTo(EVENTS);
                assertThat(report.runStatsProcessed())
                        .as("every event applied once")
                        .isEqualTo(report.processed());
                assertThat(report.duplicatesSuppressed())
                        .as("only the deliberately republished event was delivered twice")
                        .isEqualTo(1);
                for (long id : ids.subList(0, 12)) {
                    UUID eventId = tools.deadLetters().find(id).orElseThrow().eventId();
                    assertThat(tools.ledgerRows(eventId)).isEqualTo(1);
                }
                System.out.printf(
                        "OBSERVED replay: %d dead letters, %d replayed and processed once, 1 republished and"
                                + " suppressed, 10 contested ids each replayed once, 1 expired, 1 discarded%n",
                        ids.size(), replayed);
            } finally {
                tools.faultScenarios().deactivateAll();
            }
        }
    }

    private static List<Result> concurrently(ReplayService replay, List<Long> ids) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<List<Result>>> calls = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                calls.add(pool.submit(() -> {
                    go.await();
                    return replay.replay(ids);
                }));
            }
            go.countDown();
            List<Result> all = new ArrayList<>();
            for (Future<List<Result>> call : calls) {
                all.addAll(call.get(60, TimeUnit.SECONDS));
            }
            return all;
        } finally {
            pool.shutdownNow();
        }
    }

    private static List<Long> deadLetterIds(PipelineTools tools, UUID runId) {
        return tools.jdbc()
                .sql("SELECT id FROM dead_letter_events WHERE run_id = :runId ORDER BY id")
                .param("runId", runId)
                .query(Long.class)
                .list();
    }

    private static Status status(PipelineTools tools, long id) {
        DeadLetter deadLetter = tools.deadLetters().find(id).orElseThrow();
        return deadLetter.status();
    }
}
