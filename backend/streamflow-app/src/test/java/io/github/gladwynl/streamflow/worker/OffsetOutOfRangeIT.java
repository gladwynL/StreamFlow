package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Phase 0 §9 item 10: retention deleted records the group had not consumed (simulated with {@code deleteRecords},
 * which moves the log start offset exactly as size- or time-based retention does). With
 * {@code auto.offset.reset=none} the worker must report the error and must not silently skip to the new log start.
 */
class OffsetOutOfRangeIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("out-of-range"));
    private static final int PARTITION = 0;

    @Test
    void aPositionDeletedByRetentionIsReportedNotSkipped() throws Exception {
        List<EventEnvelope> events = TestRecords.events(UUID.randomUUID(), 47, 20);
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            for (EventEnvelope event : events) {
                tools.producer()
                        .send(TestRecords.record(PIPELINE.eventsTopic(), PARTITION, event))
                        .get(30, TimeUnit.SECONDS);
            }
            TopicPartition partition = new TopicPartition(PIPELINE.eventsTopic(), PARTITION);
            tools.admin()
                    .deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(10)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            assertThat(tools.committedOffsets()).containsEntry(PARTITION, 0L);

            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
                WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);
                await().atMost(Duration.ofSeconds(60)).until(() -> metrics.consumerErrors() >= 2);

                assertThat(events.subList(10, 20))
                        .as("records after the deleted range were not consumed by skipping ahead")
                        .allSatisfy(
                                e -> assertThat(tools.ledgerRows(e.eventId())).isZero());
                assertThat(tools.committedOffsets()).containsEntry(PARTITION, 0L);
                System.out.printf(
                        "OBSERVED out-of-range position: %.0f consumer errors reported, committed offset stayed 0,"
                                + " no record skipped%n",
                        metrics.consumerErrors());
            }
        }
    }
}
