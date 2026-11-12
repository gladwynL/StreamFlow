package io.github.gladwynl.streamflow.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventValidator;
import io.github.gladwynl.streamflow.persistence.BatchEventStore;
import io.github.gladwynl.streamflow.persistence.EventStore;
import io.github.gladwynl.streamflow.processing.BatchEventProcessor.BatchResult;
import io.github.gladwynl.streamflow.processing.BatchEventProcessor.RecordFailedException;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * Batch processing mode (ADR-0013) against real PostgreSQL and Kafka:
 *
 * <ul>
 *   <li>a batch is applied once; repeating it, or repeating events within it, only counts duplicates;
 *   <li>a record that cannot be decoded in the middle of a batch: the records before it are committed one by one,
 *       the failure reports its index, nothing after it is applied;
 *   <li>a failure inside the batch transaction rolls back the whole batch before the record-by-record fallback;
 *   <li>concurrent batches with overlapping events apply each event exactly once;
 *   <li>end to end, a worker crash right after the fallback has committed the records before a bad record:
 *       a restarted worker applies every event once, dead-letters the bad record once, and loses nothing.
 * </ul>
 *
 * The rest of the reliability suite also runs in batch mode (failsafe execution {@code batch-mode-it}).
 */
class BatchProcessingIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("batch"));
    private static final EventCodec CODEC = new EventCodec();

    private final JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();

    @Test
    void aBatchIsAppliedOnceAndRepeatsAreCountedAsDuplicates() {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 41, 120);
        List<ConsumerRecord<String, byte[]>> batch = records(events, 0);
        BatchEventProcessor processor = processor(FaultInjector.disabled());

        assertThat(processor.process(batch, r -> 1)).isEqualTo(new BatchResult(120, 0, false));
        assertThat(processor.process(batch, r -> 1)).isEqualTo(new BatchResult(0, 120, false));

        // A batch holding new events and repeats of the same new event.
        List<EventEnvelope> more = TestRecords.events(runId, 42, 10);
        List<ConsumerRecord<String, byte[]>> mixed = new ArrayList<>(records(more, 1_000));
        mixed.add(record(1_010, more.getFirst()));
        mixed.add(record(1_011, more.getFirst()));
        mixed.add(batch.getFirst());
        assertThat(processor.process(mixed, r -> 1)).isEqualTo(new BatchResult(10, 3, false));

        assertThat(count("SELECT count(*) FROM events WHERE run_id = :r", runId))
                .isEqualTo(130);
        assertThat(count("SELECT count(*) FROM processed_events WHERE run_id = :r", runId))
                .isEqualTo(130);
        assertThat(count("SELECT sum(processed) FROM run_stats WHERE run_id = :r", runId))
                .isEqualTo(130);
        assertThat(count("SELECT sum(duplicates_suppressed) FROM run_stats WHERE run_id = :r", runId))
                .isEqualTo(123);
    }

    @Test
    void aBadRecordInTheMiddleStopsTheBatchAtItsIndexAfterCommittingTheRecordsBeforeIt() {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 43, 10);
        List<ConsumerRecord<String, byte[]>> batch = new ArrayList<>(records(events, 0));
        batch.set(
                6,
                new ConsumerRecord<>(PIPELINE.eventsTopic(), 0, 6, "u", "not json".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> processor(FaultInjector.disabled()).process(batch, r -> 1))
                .isInstanceOfSatisfying(RecordFailedException.class, e -> {
                    assertThat(e.index()).isEqualTo(6);
                    assertThat(e.committedBefore()).isEqualTo(new BatchResult(6, 0, true));
                    assertThat(FailureClassifier.classify(e)).isEqualTo(FailureCategory.BAD_RECORD);
                });
        for (int i = 0; i < 10; i++) {
            long expected = i < 6 ? 1 : 0;
            assertThat(count(
                            "SELECT count(*) FROM processed_events WHERE event_id = :r",
                            events.get(i).eventId()))
                    .as("record %d", i)
                    .isEqualTo(expected);
        }
    }

    @Test
    void aFailureInsideTheBatchTransactionRollsBackTheWholeBatchBeforeTheFallback() {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 44, 20);
        UUID failing = events.get(12).eventId();
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
        FaultInjector faults = new FaultInjector(null, false, 0, java.time.Clock.systemUTC()) {
            @Override
            public void apply(EventEnvelope event, int deliveryAttempt) {
                if (event.eventId().equals(failing)) {
                    attempts.add(deliveryAttempt);
                    throw new IllegalStateException("simulated failure of event 12");
                }
            }
        };

        assertThatThrownBy(() -> processor(faults).process(records(events, 0), r -> 3))
                .isInstanceOfSatisfying(
                        RecordFailedException.class, e -> assertThat(e.index()).isEqualTo(12));
        // Once in the batch transaction (rolled back), once in the fallback; both with the tracked attempt.
        assertThat(attempts).containsExactly(3, 3);
        assertThat(count("SELECT count(*) FROM events WHERE run_id = :r", runId))
                .isEqualTo(12);
        assertThat(count("SELECT sum(processed) FROM run_stats WHERE run_id = :r", runId))
                .isEqualTo(12);
        assertThat(count("SELECT sum(duplicates_suppressed) FROM run_stats WHERE run_id = :r", runId))
                .isZero();
    }

    @Test
    void concurrentOverlappingBatchesApplyEachEventExactlyOnce() throws Exception {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 45, 200);
        BatchEventProcessor processor = processor(FaultInjector.disabled());
        ExecutorService pool = Executors.newFixedThreadPool(6);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < 6; t++) {
                List<EventEnvelope> shuffled = new ArrayList<>(events);
                Collections.shuffle(shuffled, new java.util.Random(t));
                List<ConsumerRecord<String, byte[]>> batch = records(shuffled, 0);
                results.add(pool.submit(() -> {
                    try {
                        return processor.process(batch, r -> 1).processed();
                    } catch (RecordFailedException e) {
                        throw new AssertionError("a concurrent batch failed record by record", e);
                    }
                }));
            }
            int applied = 0;
            for (Future<Integer> result : results) {
                applied += result.get(60, TimeUnit.SECONDS);
            }
            assertThat(applied).isEqualTo(200);
        } finally {
            pool.shutdownNow();
        }
        assertThat(count("SELECT count(*) FROM events WHERE run_id = :r", runId))
                .isEqualTo(200);
        assertThat(count("SELECT sum(processed) FROM run_stats WHERE run_id = :r", runId))
                .isEqualTo(200);
        assertThat(count("SELECT sum(duplicates_suppressed) FROM run_stats WHERE run_id = :r", runId))
                .isEqualTo(1_000);
    }

    /**
     * Phase 0 §9.7: a positioned failure plus a crash. All records are on one partition, with an undecodable record
     * at position {@code BAD}. The batch falls back to record-by-record processing, commits records 0..BAD-1 one by
     * one, and the JVM is halted right after the last of those commits, before any offset is committed. A restarted
     * worker must apply every valid event once (the BAD replayed ones as duplicates), dead-letter the bad record
     * once, and commit the partition to its end.
     */
    @Test
    void aCrashAfterTheFallbackCommitsLosesNothingAndAppliesNothingTwice() throws Exception {
        Pipeline pipeline = IntegrationTestInfrastructure.provision(Pipeline.named("batch-crash"));
        final int bad = 7;
        final int total = 30;
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 46, total - 1);
        try (PipelineTools tools = new PipelineTools(pipeline)) {
            for (int i = 0; i < total; i++) {
                ProducerRecord<String, byte[]> record = i == bad
                        ? new ProducerRecord<>(
                                pipeline.eventsTopic(), 0, "bad", "{broken".getBytes(StandardCharsets.UTF_8))
                        : TestRecords.record(pipeline.eventsTopic(), 0, events.get(i < bad ? i : i - 1));
                tools.producer().send(record).get();
            }

            Path log = Path.of("target", "batch-crash-worker.log");
            Process worker = new ProcessBuilder(IntegrationTestInfrastructure.workerProcessCommand(
                            pipeline,
                            "--streamflow.worker.processing-mode=BATCH",
                            "--streamflow.crash-test.point=AFTER_DB_COMMIT",
                            "--streamflow.crash-test.after-records=" + bad))
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
            assertThat(worker.exitValue()).as("see " + log).isEqualTo(CrashInjector.CRASH_EXIT_CODE);
            assertThat(Files.readString(log)).contains(CrashInjector.CRASH_MARKER);
            assertThat(count("SELECT count(*) FROM processed_events WHERE run_id = :r", runId))
                    .isEqualTo(bad);
            assertThat(tools.committedOffsets().getOrDefault(0, 0L))
                    .as("no offset is committed before the crash")
                    .isZero();

            try (ConfigurableApplicationContext recovery =
                    IntegrationTestInfrastructure.startWorker(pipeline, "--streamflow.worker.processing-mode=BATCH")) {
                await().atMost(Duration.ofSeconds(60))
                        .until(() -> tools.committedOffsets().getOrDefault(0, 0L) == total);
            }
            assertThat(count("SELECT count(*) FROM events WHERE run_id = :r", runId))
                    .isEqualTo(total - 1);
            assertThat(count("SELECT sum(processed) FROM run_stats WHERE run_id = :r", runId))
                    .isEqualTo(total - 1);
            assertThat(count("SELECT sum(duplicates_suppressed) FROM run_stats WHERE run_id = :r", runId))
                    .as("the records committed before the crash are redelivered and suppressed")
                    .isEqualTo(bad);
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> jdbc.sql("SELECT count(*) FROM dead_letter_events WHERE source_topic = :t")
                                    .param("t", pipeline.eventsTopic())
                                    .query(Long.class)
                                    .single()
                            == 1);
            Map<String, Object> dead = jdbc.sql(
                            "SELECT source_offset, failure_category FROM dead_letter_events WHERE source_topic = :t")
                    .param("t", pipeline.eventsTopic())
                    .query()
                    .singleRow();
            assertThat(((Number) dead.get("source_offset")).longValue()).isEqualTo(bad);
            assertThat(dead.get("failure_category")).isEqualTo("BAD_RECORD");
        }
    }

    private BatchEventProcessor processor(FaultInjector faults) {
        DataSourceTransactionManager transactions =
                new DataSourceTransactionManager(IntegrationTestInfrastructure.dataSource());
        EventProcessor records = new EventProcessor(
                CODEC, new EventValidator(), new EventStore(jdbc), transactions, CrashInjector.disabled(), faults);
        return new BatchEventProcessor(
                CODEC,
                new EventValidator(),
                new BatchEventStore(new JdbcTemplate(IntegrationTestInfrastructure.dataSource())),
                records,
                transactions,
                CrashInjector.disabled(),
                faults);
    }

    private static List<ConsumerRecord<String, byte[]>> records(List<EventEnvelope> events, long firstOffset) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            records.add(record(firstOffset + i, events.get(i)));
        }
        return records;
    }

    private static ConsumerRecord<String, byte[]> record(long offset, EventEnvelope event) {
        return new ConsumerRecord<>(PIPELINE.eventsTopic(), 0, offset, event.userId(), CODEC.encode(event));
    }

    private long count(String sql, UUID id) {
        Long value = jdbc.sql(sql).param("r", id).query(Long.class).single();
        return value == null ? 0 : value;
    }
}
