package io.github.gladwynl.streamflow.worker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.simulation.EventContentGenerator;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import java.time.Instant;
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
 * Phase 2 failure behaviour (no dead-letter topic yet): a record that can never be processed is retried without
 * limit. Verifies Phase 0 §9 items 1 and 2:
 *
 * <ul>
 *   <li>the stalled partition never advances past the bad record, and nothing after it is processed or skipped;
 *   <li>partitions owned by <em>other</em> consumer threads keep flowing;
 *   <li>observed, not asserted: how a partition owned by the <em>same</em> consumer thread fares (head-of-line
 *       blocking), and whether records before the bad one were committed before the seek. Correctness depends on
 *       neither, because of the ledger.
 * </ul>
 */
class PartitionStallIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("stall"));
    private static final int STALLED = 0;
    private static final long POISON_OFFSET = 5;

    @Test
    void anUnprocessableRecordStallsItsPartitionAndIsNeverSkipped() throws Exception {
        UUID runId = UUID.randomUUID();
        List<EventEnvelope> events = events(runId, 20);
        List<EventEnvelope> beforePoison = events.subList(0, 5);
        List<EventEnvelope> afterPoison = events.subList(5, 10);
        List<EventEnvelope> sameConsumer = events.subList(10, 15);
        List<EventEnvelope> otherConsumer = events.subList(15, 20);
        EventCodec codec = new EventCodec();

        try (PipelineTools tools = new PipelineTools(PIPELINE);
                ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
            Map<Integer, Integer> owner = awaitAssignment(worker);
            int samePartition = partitionOwnedBy(owner, owner.get(STALLED), STALLED);
            int otherPartition = partitionNotOwnedBy(owner, owner.get(STALLED));

            List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
            beforePoison.forEach(e -> records.add(record(STALLED, e, codec)));
            records.add(
                    new ProducerRecord<>(PIPELINE.eventsTopic(), STALLED, "poison", "not an event".getBytes(UTF_8)));
            afterPoison.forEach(e -> records.add(record(STALLED, e, codec)));
            sameConsumer.forEach(e -> records.add(record(samePartition, e, codec)));
            otherConsumer.forEach(e -> records.add(record(otherPartition, e, codec)));
            for (ProducerRecord<String, byte[]> record : records) {
                tools.producer().send(record).get(30, TimeUnit.SECONDS);
            }

            await().atMost(Duration.ofSeconds(60))
                    .until(() -> allProcessed(tools, beforePoison) && allProcessed(tools, otherConsumer));

            // Several retry cycles (0.5 s, 1 s, 2 s, 4 s) pass without the partition moving on.
            await().during(Duration.ofSeconds(8))
                    .atMost(Duration.ofSeconds(12))
                    .until(() -> noneProcessed(tools, afterPoison));

            Map<Integer, Long> committed = tools.committedOffsets();
            long stalledCommitted = committed.getOrDefault(STALLED, 0L);
            assertThat(stalledCommitted)
                    .as("never committed past the unprocessable record")
                    .isLessThanOrEqualTo(POISON_OFFSET);
            assertThat(committed).containsEntry(otherPartition, 5L);
            System.out.printf(
                    "OBSERVED stall: partition %d committed offset %d (bad record at %d); same-consumer partition %d"
                            + " processed %d of 5, committed %s; other-consumer partition %d processed 5 of 5%n",
                    STALLED,
                    stalledCommitted,
                    POISON_OFFSET,
                    samePartition,
                    sameConsumer.stream()
                            .filter(e -> tools.ledgerRows(e.eventId()) == 1)
                            .count(),
                    committed.get(samePartition),
                    otherPartition);
        }
    }

    /** Partition → index of the consumer thread that owns it, once all partitions are assigned. */
    private static Map<Integer, Integer> awaitAssignment(ConfigurableApplicationContext worker) {
        KafkaListenerEndpointRegistry registry = worker.getBean(KafkaListenerEndpointRegistry.class);
        ConcurrentMessageListenerContainer<?, ?> container =
                (ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(EventListener.LISTENER_ID);
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

    private static int partitionOwnedBy(Map<Integer, Integer> owner, int consumer, int except) {
        return owner.entrySet().stream()
                .filter(e -> e.getValue() == consumer && e.getKey() != except)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("consumer " + consumer + " owns only one partition: " + owner));
    }

    private static int partitionNotOwnedBy(Map<Integer, Integer> owner, int consumer) {
        return owner.entrySet().stream()
                .filter(e -> e.getValue() != consumer)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("one consumer owns every partition: " + owner));
    }

    private static ProducerRecord<String, byte[]> record(int partition, EventEnvelope event, EventCodec codec) {
        return new ProducerRecord<>(PIPELINE.eventsTopic(), partition, event.userId(), codec.encode(event));
    }

    private static List<EventEnvelope> events(UUID runId, int count) {
        EventContentGenerator generator = new EventContentGenerator(new SimulationSpec(3, count, 1_000, 100));
        EventIds ids = new EventIds();
        Instant start = Instant.now();
        List<EventEnvelope> events = new ArrayList<>();
        while (generator.hasNext()) {
            events.add(generator.next().toEnvelope(runId, ids.next(), start));
        }
        return events;
    }

    private static boolean allProcessed(PipelineTools tools, List<EventEnvelope> events) {
        return events.stream().allMatch(e -> tools.ledgerRows(e.eventId()) == 1);
    }

    private static boolean noneProcessed(PipelineTools tools, List<EventEnvelope> events) {
        return events.stream().allMatch(e -> tools.ledgerRows(e.eventId()) == 0);
    }
}
