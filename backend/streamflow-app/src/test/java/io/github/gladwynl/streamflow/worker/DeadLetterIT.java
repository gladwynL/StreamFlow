package io.github.gladwynl.streamflow.worker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventHeaders;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.DeadLetter;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.Status;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

/**
 * Bad records are dead-lettered on the first attempt with everything needed to inspect and replay them, and the
 * partition moves on. Phase 2's head-of-line blocking of partitions sharing the consumer thread is gone.
 */
class DeadLetterIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("dead-letter"));
    private static final int BAD = 0;

    @Test
    void badRecordsAreDeadLetteredOnceWithProvenanceAndProcessingContinues() throws Exception {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = TestRecords.events(runId, 17, 11);
        EventEnvelope invalid = withUserId(events.get(10), "not-a-user");

        try (PipelineTools tools = new PipelineTools(PIPELINE);
                ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            Map<Integer, Integer> owner = awaitAssignment(worker);
            int sameConsumer = owner.entrySet().stream()
                    .filter(e -> e.getKey() != BAD && e.getValue().equals(owner.get(BAD)))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow();

            List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
            events.subList(0, 3).forEach(e -> records.add(TestRecords.record(PIPELINE.eventsTopic(), BAD, e)));
            byte[] garbage = "not an event".getBytes(UTF_8);
            records.add(new ProducerRecord<>(PIPELINE.eventsTopic(), BAD, "garbage-key", garbage)); // offset 3
            records.add(TestRecords.record(PIPELINE.eventsTopic(), BAD, invalid)); // offset 4
            events.subList(3, 6).forEach(e -> records.add(TestRecords.record(PIPELINE.eventsTopic(), BAD, e)));
            events.subList(6, 10)
                    .forEach(e -> records.add(TestRecords.record(PIPELINE.eventsTopic(), sameConsumer, e)));
            for (ProducerRecord<String, byte[]> record : records) {
                tools.producer().send(record).get(30, TimeUnit.SECONDS);
            }

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                assertThat(events.subList(0, 10))
                        .allSatisfy(
                                e -> assertThat(tools.ledgerRows(e.eventId())).isEqualTo(1));
                assertThat(deadLetter(tools, 3)).isNotNull();
                assertThat(deadLetter(tools, 4)).isNotNull();
                assertThat(tools.committedOffsets()).containsEntry(BAD, 8L).containsEntry(sameConsumer, 4L);
            });

            DeadLetter undecodable = deadLetter(tools, 3);
            assertThat(undecodable.status()).isEqualTo(Status.PENDING);
            assertThat(undecodable.failureCategory()).isEqualTo("BAD_RECORD");
            assertThat(undecodable.exceptionClass()).endsWith("EventDecodingException");
            assertThat(undecodable.errorMessage()).contains("Malformed event JSON");
            assertThat(undecodable.attempts()).isEqualTo(1);
            assertThat(undecodable.payload()).isEqualTo(garbage);
            assertThat(undecodable.recordKey()).isEqualTo("garbage-key");
            assertThat(undecodable.eventId())
                    .as("no identity headers on this record")
                    .isNull();

            DeadLetter invalidEvent = deadLetter(tools, 4);
            assertThat(invalidEvent.failureCategory()).isEqualTo("BAD_RECORD");
            assertThat(invalidEvent.exceptionClass()).endsWith("EventValidationException");
            assertThat(invalidEvent.errorMessage()).contains("userId has an invalid format");
            assertThat(invalidEvent.eventId()).isEqualTo(invalid.eventId());
            assertThat(invalidEvent.runId()).isEqualTo(runId);
            assertThat(invalidEvent.runSeq()).isEqualTo(invalid.sequence());
            assertThat(invalidEvent.payload()).isEqualTo(TestRecords.encode(invalid));
            assertThat(invalidEvent.headersJson())
                    .contains(EventHeaders.EVENT_ID)
                    .contains("kafka_dlt-original-offset")
                    .doesNotContain("stacktrace");

            assertThat(tools.deadLetterTopicRecords())
                    .as("exactly one dead letter per bad record")
                    .isEqualTo(2);
            WorkerMetrics metrics = worker.getBean(WorkerMetrics.class);
            assertThat(metrics.deadLetteredCount(FailureCategory.BAD_RECORD)).isEqualTo(2);
            assertThat(metrics.failures(FailureCategory.BAD_RECORD)).isEqualTo(2);
        }
    }

    private static DeadLetter deadLetter(PipelineTools tools, long offset) {
        List<Long> ids = tools.jdbc()
                .sql("SELECT id FROM dead_letter_events WHERE source_topic = :topic AND source_partition = :p"
                        + " AND source_offset = :offset")
                .param("topic", PIPELINE.eventsTopic())
                .param("p", BAD)
                .param("offset", offset)
                .query(Long.class)
                .list();
        return ids.isEmpty() ? null : tools.deadLetters().find(ids.getFirst()).orElseThrow();
    }

    private static EventEnvelope withUserId(EventEnvelope e, String userId) {
        return new EventEnvelope(
                e.schemaVersion(),
                e.eventId(),
                e.eventType(),
                e.occurredAt(),
                e.source(),
                e.synthetic(),
                e.runId(),
                e.sequence(),
                userId,
                e.sessionId(),
                e.payload());
    }

    /** Partition → index of the consumer thread that owns it, once all partitions are assigned. */
    static Map<Integer, Integer> awaitAssignment(ConfigurableApplicationContext worker) {
        ConcurrentMessageListenerContainer<?, ?> container = (ConcurrentMessageListenerContainer<?, ?>)
                worker.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer(EventListener.LISTENER_ID);
        Map<Integer, Integer> owner = new HashMap<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            owner.clear();
            List<? extends KafkaMessageListenerContainer<?, ?>> consumers = container.getContainers();
            for (int i = 0; i < consumers.size(); i++) {
                Collection<TopicPartition> assigned = consumers.get(i).getAssignedPartitions();
                if (assigned != null) {
                    for (TopicPartition tp : assigned) {
                        owner.put(tp.partition(), i);
                    }
                }
            }
            return owner.size() == Pipeline.PARTITIONS;
        });
        return Map.copyOf(owner);
    }
}
