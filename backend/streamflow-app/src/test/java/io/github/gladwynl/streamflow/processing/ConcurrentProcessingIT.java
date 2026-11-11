package io.github.gladwynl.streamflow.processing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.events.EventValidator;
import io.github.gladwynl.streamflow.persistence.EventStore;
import io.github.gladwynl.streamflow.simulation.EventContentGenerator;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * Two consumers can briefly hold the same record (for example during a rebalance). The ledger's primary key must
 * make exactly one of the concurrent transactions apply the event, against real PostgreSQL.
 */
class ConcurrentProcessingIT {

    private static final int THREADS = 8;
    private static final int EVENTS = 50;

    @Test
    void concurrentDeliveriesOfTheSameEventTakeEffectExactlyOnce() throws Exception {
        JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();
        EventProcessor processor = new EventProcessor(
                new EventCodec(),
                new EventValidator(),
                new EventStore(jdbc),
                new DataSourceTransactionManager(IntegrationTestInfrastructure.dataSource()),
                CrashInjector.disabled());
        UUID runId = UUID.randomUUID();
        EventContentGenerator generator = new EventContentGenerator(new SimulationSpec(5, EVENTS, 1_000, 100));
        EventIds ids = new EventIds();
        EventCodec codec = new EventCodec();
        Instant start = Instant.now();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);

        try {
            for (long offset = 0; generator.hasNext(); offset++) {
                EventEnvelope event = generator.next().toEnvelope(runId, ids.next(), start);
                ConsumerRecord<String, byte[]> record = record(offset, event, codec);

                CountDownLatch go = new CountDownLatch(1);
                List<Future<ProcessingResult>> attempts = new ArrayList<>();
                for (int t = 0; t < THREADS; t++) {
                    attempts.add(pool.submit(() -> {
                        go.await();
                        return processor.process(record);
                    }));
                }
                go.countDown();

                List<ProcessingResult> results = new ArrayList<>();
                for (Future<ProcessingResult> attempt : attempts) {
                    results.add(attempt.get(30, TimeUnit.SECONDS));
                }
                assertThat(results)
                        .filteredOn(r -> r == ProcessingResult.PROCESSED)
                        .hasSize(1);
                assertThat(results)
                        .filteredOn(r -> r == ProcessingResult.DUPLICATE)
                        .hasSize(THREADS - 1);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(count(jdbc, "SELECT count(*) FROM processed_events WHERE run_id = :runId", runId))
                .isEqualTo(EVENTS);
        assertThat(count(jdbc, "SELECT count(*) FROM events WHERE run_id = :runId", runId))
                .isEqualTo(EVENTS);
        assertThat(count(jdbc, "SELECT sum(processed) FROM run_stats WHERE run_id = :runId", runId))
                .isEqualTo(EVENTS);
        assertThat(count(jdbc, "SELECT sum(duplicates_suppressed) FROM run_stats WHERE run_id = :runId", runId))
                .isEqualTo((long) EVENTS * (THREADS - 1));
    }

    private static ConsumerRecord<String, byte[]> record(long offset, EventEnvelope event, EventCodec codec) {
        byte[] value = codec.encode(event);
        return new ConsumerRecord<>(
                "it.concurrent",
                0,
                offset,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                event.userId().length(),
                value.length,
                event.userId(),
                value,
                new RecordHeaders(),
                Optional.empty());
    }

    private static long count(JdbcClient jdbc, String sql, UUID runId) {
        return jdbc.sql(sql).param("runId", runId).query(Long.class).single();
    }
}
