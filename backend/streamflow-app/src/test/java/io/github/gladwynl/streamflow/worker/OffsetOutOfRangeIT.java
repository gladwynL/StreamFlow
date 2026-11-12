package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Phase 0 §9 item 10 and Phase 6 offset recovery: retention deleted records the group had not consumed (simulated
 * with {@code deleteRecords}, which moves the log start offset exactly as size- or time-based retention does).
 *
 * <ul>
 *   <li>the worker pauses the affected partition after the first error instead of failing on every poll;
 *   <li>it skips nothing and moves no offset; other partitions keep processing;
 *   <li>the gap (group, partition, lost position, log start, events lost) is recorded and system status reports it
 *       with operator guidance;
 *   <li>only a deliberate operator reset (workers stopped, offset moved to the log start, as {@code
 *       kafka-consumer-groups --reset-offsets --to-earliest} does) lets the partition continue; the gap stays
 *       recorded and run reconciliation still reports every lost event as missing.
 * </ul>
 */
class OffsetOutOfRangeIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("out-of-range"));
    private static final int GAP_PARTITION = 0;
    private static final int OTHER_PARTITION = 1;
    private static final int PER_PARTITION = 20;
    private static final int DELETED = 10;

    @Test
    void aLostPositionPausesThePartitionIsRecordedAndNeedsADeliberateOperatorReset() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            UUID runId = UUID.randomUUID();
            int total = 2 * PER_PARTITION;
            tools.runs().create(runId, 47, 1, total, 1_000, 500, PIPELINE.eventsTopic());
            List<EventEnvelope> events = TestRecords.events(runId, 47, total);
            for (int i = 0; i < total; i++) {
                int partition = i < PER_PARTITION ? GAP_PARTITION : OTHER_PARTITION;
                tools.producer()
                        .send(TestRecords.record(PIPELINE.eventsTopic(), partition, events.get(i)))
                        .get(30, TimeUnit.SECONDS);
            }
            tools.runs().finish(runId, RunStatus.COMPLETED, null, total, total, 0, 0);
            TopicPartition gapPartition = new TopicPartition(PIPELINE.eventsTopic(), GAP_PARTITION);
            tools.admin()
                    .deleteRecords(Map.of(gapPartition, RecordsToDelete.beforeOffset(DELETED)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            assertThat(tools.committedOffsets()).containsEntry(GAP_PARTITION, 0L);

            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE);
                    ConfigurableApplicationContext api = IntegrationTestInfrastructure.startApi(PIPELINE)) {
                WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);

                // The gap is recorded with its exact extent.
                await().atMost(Duration.ofSeconds(60)).until(() -> gapRows(tools) == 1);
                Map<String, Object> gap = tools.jdbc()
                        .sql("SELECT lost_position, log_start_offset, events_lost FROM offset_gaps"
                                + " WHERE consumer_group = :g AND topic = :t AND kafka_partition = :p")
                        .param("g", PIPELINE.consumerGroup())
                        .param("t", PIPELINE.eventsTopic())
                        .param("p", GAP_PARTITION)
                        .query()
                        .singleRow();
                assertThat(((Number) gap.get("lost_position")).longValue()).isZero();
                assertThat(((Number) gap.get("log_start_offset")).longValue()).isEqualTo(DELETED);
                assertThat(((Number) gap.get("events_lost")).longValue()).isEqualTo(DELETED);

                // Other partitions keep processing.
                await().atMost(Duration.ofSeconds(60))
                        .until(() -> events.subList(PER_PARTITION, total).stream()
                                .allMatch(e -> tools.ledgerRows(e.eventId()) == 1));

                // No error loop: once paused, the partition produces no further errors.
                double errors = metrics.consumerErrors();
                assertThat(errors).as("errors before the partition was paused").isBetween(1.0, 3.0);
                await().during(Duration.ofSeconds(8))
                        .atMost(Duration.ofSeconds(12))
                        .until(() -> metrics.consumerErrors() == errors);

                // Nothing was skipped and no offset moved on the gap partition.
                assertThat(tools.committedOffsets()).containsEntry(GAP_PARTITION, 0L);
                assertThat(events.subList(DELETED, PER_PARTITION))
                        .allSatisfy(
                                e -> assertThat(tools.ledgerRows(e.eventId())).isZero());

                // System status reports the open gap with operator guidance.
                String status = status(api);
                assertThat(status).contains("\"status\":\"DEGRADED\"");
                assertThat(status).contains("Offset gap on " + PIPELINE.eventsTopic() + "-0");
                assertThat(status).contains("10 events lost");
                assertThat(status).contains("\"open\":true");
            }

            // Deliberate operator recovery with the workers stopped: move the group to the first available offset.
            tools.admin()
                    .alterConsumerGroupOffsets(
                            PIPELINE.consumerGroup(), Map.of(gapPartition, new OffsetAndMetadata(DELETED)))
                    .all()
                    .get(30, TimeUnit.SECONDS);

            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE);
                    ConfigurableApplicationContext api = IntegrationTestInfrastructure.startApi(PIPELINE)) {
                await().atMost(Duration.ofSeconds(60))
                        .until(() -> tools.committedOffsets().getOrDefault(GAP_PARTITION, 0L) == PER_PARTITION);
                assertThat(events.subList(DELETED, PER_PARTITION))
                        .as("the surviving events of the gap partition are processed once")
                        .allSatisfy(
                                e -> assertThat(tools.ledgerRows(e.eventId())).isEqualTo(1));
                assertThat(events.subList(0, DELETED))
                        .as("the deleted events were never processed")
                        .allSatisfy(
                                e -> assertThat(tools.ledgerRows(e.eventId())).isZero());

                assertThat(gapRows(tools))
                        .as("the gap stays recorded as evidence")
                        .isEqualTo(1);
                String status = status(api);
                assertThat(status).contains("\"open\":false");
                assertThat(status).doesNotContain("Offset gap on");
            }

            ReconciliationReport report = tools.reconciliation().reconcile(runId);
            assertThat(report.result()).isEqualTo(ReconciliationReport.Result.FAIL);
            assertThat(report.missing())
                    .as("the lost events are reported missing")
                    .isEqualTo(DELETED);
            assertThat(report.missingSample())
                    .containsExactlyElementsOf(events.subList(0, DELETED).stream()
                            .map(EventEnvelope::sequence)
                            .toList());
            System.out.printf(
                    "OBSERVED offset gap: partition paused after the first error, %d events recorded lost,"
                            + " operator reset processed the %d survivors, reconciliation FAIL with %d missing%n",
                    DELETED, PER_PARTITION - DELETED, report.missing());
        }
    }

    private static long gapRows(PipelineTools tools) {
        return tools.jdbc()
                .sql("SELECT count(*) FROM offset_gaps WHERE consumer_group = :g AND topic = :t")
                .param("g", PIPELINE.consumerGroup())
                .param("t", PIPELINE.eventsTopic())
                .query(Long.class)
                .single();
    }

    private static String status(ConfigurableApplicationContext api) throws Exception {
        String port = api.getEnvironment().getProperty("local.server.port");
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/system/status"))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
