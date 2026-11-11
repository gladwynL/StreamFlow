package io.github.gladwynl.streamflow.processing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Kills a real worker JVM with {@link Runtime#halt} at each crash point (ADR-0005), then restarts processing and
 * reconciles. Halting skips every shutdown hook, so neither Spring nor the Kafka client can commit anything: this is
 * a process crash, not a graceful stop.
 *
 * <p>The run is fully published before the crashing worker starts, so the crash happens in the middle of a steady
 * stream of polls.
 */
class CrashRecoveryIT {

    private static final int EVENTS = 3_000;
    private static final int CRASH_AFTER_RECORDS = 1_000;
    private static final Pattern CRASH_LINE =
            Pattern.compile(CrashInjector.CRASH_MARKER + " point=(\\S+) eventId=(\\S+) partition=(\\d+) offset=(\\d+)");

    @Test
    void crashBeforeTheDatabaseCommitRollsBackAndTheEventIsProcessedOnceAfterRestart() throws Exception {
        Evidence evidence = crashAndRecover("crash-before", "BEFORE_DB_COMMIT");

        assertThat(evidence.ledgerRowsAfterCrash())
                .as("the crashed transaction was rolled back")
                .isZero();
        assertThat(evidence.committedAfterCrash())
                .as("no offset was committed past the crashed record")
                .isLessThanOrEqualTo(evidence.crashOffset());
        assertThat(evidence.ledgerRowsAfterRecovery()).isEqualTo(1);
        assertReconciled(evidence.report());
        System.out.printf(
                "OBSERVED before-commit crash: record p%d@%d, committed offset %d, %d duplicate deliveries suppressed%n",
                evidence.crashPartition(),
                evidence.crashOffset(),
                evidence.committedAfterCrash(),
                evidence.report().duplicatesSuppressed());
    }

    @Test
    void crashAfterTheDatabaseCommitIsRedeliveredAndSuppressedByTheLedger() throws Exception {
        Evidence evidence = crashAndRecover("crash-after", "AFTER_DB_COMMIT");

        assertThat(evidence.ledgerRowsAfterCrash())
                .as("the transaction committed before the crash")
                .isEqualTo(1);
        assertThat(evidence.committedAfterCrash())
                .as("the database commit happened, but the offset commit did not")
                .isLessThanOrEqualTo(evidence.crashOffset());
        assertThat(evidence.ledgerRowsAfterRecovery()).isEqualTo(1);
        assertReconciled(evidence.report());
        assertThat(evidence.report().duplicatesSuppressed())
                .as("at least the crashed record was redelivered and suppressed")
                .isPositive();
        System.out.printf(
                "OBSERVED after-commit crash: record p%d@%d, committed offset %d, %d duplicate deliveries suppressed%n",
                evidence.crashPartition(),
                evidence.crashOffset(),
                evidence.committedAfterCrash(),
                evidence.report().duplicatesSuppressed());
    }

    /**
     * Requirement 7: the worker halts after the dead-letter record is acknowledged but before the source offset is
     * committed. After restart the bad record is processed again and dead-lettered a second time; the dead-letter
     * topic holds two copies, but PostgreSQL stores one dead letter because the source coordinates are unique.
     */
    @Test
    void crashAfterDeadLetterPublicationDoesNotDuplicateTheStoredDeadLetter() throws Exception {
        Pipeline pipeline = IntegrationTestInfrastructure.provision(Pipeline.named("crash-dlt"));
        try (PipelineTools tools = new PipelineTools(pipeline)) {
            tools.producer()
                    .send(new ProducerRecord<>(pipeline.eventsTopic(), 0, "bad", "not an event".getBytes(UTF_8)))
                    .get(30, TimeUnit.SECONDS);

            Path log = Path.of("target", "crash-dlt-worker.log");
            Process worker = new ProcessBuilder(IntegrationTestInfrastructure.workerProcessCommand(
                            pipeline,
                            "--streamflow.crash-test.point=AFTER_DLT_PUBLISH",
                            "--streamflow.crash-test.after-records=1"))
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile())
                    .start();
            try {
                assertThat(worker.waitFor(120, TimeUnit.SECONDS))
                        .as("worker should crash; see " + log)
                        .isTrue();
            } finally {
                worker.destroyForcibly();
            }
            assertThat(worker.exitValue()).as("exit status; see " + log).isEqualTo(CrashInjector.CRASH_EXIT_CODE);
            assertThat(crashLine(log).group(1)).isEqualTo("AFTER_DLT_PUBLISH");

            assertThat(tools.deadLetterTopicRecords())
                    .as("the dead letter was acknowledged")
                    .isEqualTo(1);
            assertThat(tools.committedOffsets().getOrDefault(0, 0L))
                    .as("but the source offset was not committed")
                    .isZero();

            try (ConfigurableApplicationContext recovery = IntegrationTestInfrastructure.startWorker(pipeline)) {
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(tools.committedOffsets()).containsEntry(0, 1L);
                    assertThat(tools.deadLetterTopicRecords())
                            .as("published again after restart")
                            .isEqualTo(2);
                    assertThat(tools.lag(pipeline.ingestorGroup(), pipeline.deadLetterSpec()))
                            .isZero();
                });
            }
            long stored = tools.jdbc()
                    .sql("SELECT count(*) FROM dead_letter_events WHERE source_topic = :topic")
                    .param("topic", pipeline.eventsTopic())
                    .query(Long.class)
                    .single();
            assertThat(stored)
                    .as("one stored dead letter for one source record")
                    .isEqualTo(1);
            System.out.println("OBSERVED crash after dead-letter publication: 2 dead-letter records, 1 stored row");
        }
    }

    private static Evidence crashAndRecover(String name, String crashPoint) throws Exception {
        Pipeline pipeline = IntegrationTestInfrastructure.provision(Pipeline.named(name));
        try (PipelineTools tools = new PipelineTools(pipeline)) {
            RunSummary run = tools.simulation().run(new SimulationSpec(31, EVENTS, 10_000, 50_000));
            assertThat(run.acked()).isEqualTo(EVENTS);

            Path log = Path.of("target", name + "-worker.log");
            Process worker = new ProcessBuilder(IntegrationTestInfrastructure.workerProcessCommand(
                            pipeline,
                            "--streamflow.crash-test.point=" + crashPoint,
                            "--streamflow.crash-test.after-records=" + CRASH_AFTER_RECORDS))
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile())
                    .start();
            try {
                assertThat(worker.waitFor(120, TimeUnit.SECONDS))
                        .as("worker should crash; see " + log)
                        .isTrue();
            } finally {
                worker.destroyForcibly();
            }
            assertThat(worker.exitValue()).as("exit status; see " + log).isEqualTo(CrashInjector.CRASH_EXIT_CODE);

            Matcher crash = crashLine(log);
            assertThat(crash.group(1)).isEqualTo(crashPoint);
            UUID eventId = UUID.fromString(crash.group(2));
            int partition = Integer.parseInt(crash.group(3));
            long offset = Long.parseLong(crash.group(4));

            long ledgerRowsAfterCrash = tools.ledgerRows(eventId);
            Map<Integer, Long> committed = tools.committedOffsets();
            long committedAfterCrash = committed.getOrDefault(partition, 0L);

            ReconciliationReport report;
            try (ConfigurableApplicationContext recovery = IntegrationTestInfrastructure.startWorker(pipeline)) {
                report = tools.reconciliation().awaitDrainAndReconcile(run.runId(), Duration.ofSeconds(120));
            }
            return new Evidence(
                    partition, offset, ledgerRowsAfterCrash, committedAfterCrash, tools.ledgerRows(eventId), report);
        }
    }

    private static Matcher crashLine(Path log) throws IOException {
        for (String line : Files.readAllLines(log)) {
            Matcher matcher = CRASH_LINE.matcher(line);
            if (matcher.find()) {
                return matcher;
            }
        }
        throw new AssertionError("No " + CrashInjector.CRASH_MARKER + " line in " + log);
    }

    private static void assertReconciled(ReconciliationReport report) {
        assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
        assertThat(report.processed()).isEqualTo(EVENTS);
        assertThat(report.eventRows()).isEqualTo(EVENTS);
        assertThat(report.runStatsProcessed())
                .as("effects applied once per event")
                .isEqualTo(EVENTS);
    }

    private record Evidence(
            int crashPartition,
            long crashOffset,
            long ledgerRowsAfterCrash,
            long committedAfterCrash,
            long ledgerRowsAfterRecovery,
            ReconciliationReport report) {}
}
