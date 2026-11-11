package io.github.gladwynl.streamflow.worker;

import static java.nio.charset.StandardCharsets.UTF_8;
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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The broker rejects dead-letter records (the dead-letter topic's {@code max.message.bytes} is temporarily set
 * below the record size). Verifies requirement 6 and Phase 0 §9 items 3 and 4: while publication fails, the bad
 * record is redelivered, its offset is never committed, and nothing after it on the partition is processed. Once
 * the topic accepts records again, the dead letter is published once and the partition moves on.
 */
class DeadLetterPublishFailureIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("dlt-failure"));
    private static final int PARTITION = 0;

    @Test
    void failedDeadLetterPublicationNeverAdvancesTheSourceOffset() throws Exception {
        EventEnvelope after = TestRecords.events(UUID.randomUUID(), 5, 1).getFirst();
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            setDeadLetterMaxMessageBytes(tools, "64");
            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
                WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);
                tools.producer()
                        .send(new ProducerRecord<>(
                                PIPELINE.eventsTopic(),
                                PARTITION,
                                "bad",
                                "x".repeat(200).getBytes(UTF_8)))
                        .get(30, TimeUnit.SECONDS); // offset 0
                tools.producer()
                        .send(TestRecords.record(PIPELINE.eventsTopic(), PARTITION, after))
                        .get(30, TimeUnit.SECONDS); // offset 1

                // Several rejected publications, each followed by redelivery of the bad record.
                await().atMost(Duration.ofSeconds(60)).until(() -> metrics.deadLetterPublishFailures() >= 3);
                assertThat(tools.committedOffsets().getOrDefault(PARTITION, 0L))
                        .as("the bad record's offset was never committed")
                        .isZero();
                assertThat(tools.ledgerRows(after.eventId()))
                        .as("nothing after the bad record was processed")
                        .isZero();
                assertThat(tools.deadLetterTopicRecords()).isZero();
                double failuresWhileRejected = metrics.deadLetterPublishFailures();

                setDeadLetterMaxMessageBytes(tools, null);

                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(tools.ledgerRows(after.eventId())).isEqualTo(1);
                    assertThat(tools.committedOffsets()).containsEntry(PARTITION, 2L);
                    assertThat(deadLetterRows(tools)).isEqualTo(1);
                });
                assertThat(tools.deadLetterTopicRecords())
                        .as("published exactly once")
                        .isEqualTo(1);
                System.out.printf(
                        "OBSERVED dead-letter publication failure: %.0f rejected publications, committed offset"
                                + " stayed 0 until the dead letter was acknowledged, then moved to 2%n",
                        failuresWhileRejected);
            } finally {
                setDeadLetterMaxMessageBytes(tools, null);
            }
        }
    }

    private static long deadLetterRows(PipelineTools tools) {
        return tools.jdbc()
                .sql("SELECT count(*) FROM dead_letter_events WHERE source_topic = :topic AND source_partition = 0"
                        + " AND source_offset = 0")
                .param("topic", PIPELINE.eventsTopic())
                .query(Long.class)
                .single();
    }

    /** Sets (or, with null, removes) the topic override and waits until the broker reports it. */
    private static void setDeadLetterMaxMessageBytes(PipelineTools tools, String value) throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, PIPELINE.deadLetterTopic());
        AlterConfigOp op = value == null
                ? new AlterConfigOp(new ConfigEntry("max.message.bytes", ""), AlterConfigOp.OpType.DELETE)
                : new AlterConfigOp(new ConfigEntry("max.message.bytes", value), AlterConfigOp.OpType.SET);
        tools.admin().incrementalAlterConfigs(Map.of(topic, List.of(op))).all().get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Config config = tools.admin()
                    .describeConfigs(List.of(topic))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic);
            String current = config.get("max.message.bytes").value();
            if (value == null) {
                assertThat(current).isNotEqualTo("64");
            } else {
                assertThat(current).isEqualTo(value);
            }
        });
    }
}
