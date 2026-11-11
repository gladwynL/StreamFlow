package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.simulation.EventContentGenerator;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Events and Kafka records built exactly as the generator builds them. */
public final class TestRecords {

    private static final EventCodec CODEC = new EventCodec();

    private TestRecords() {}

    public static List<EventEnvelope> events(UUID runId, long seed, int count) {
        EventContentGenerator generator = new EventContentGenerator(new SimulationSpec(seed, count, 1_000, 500));
        EventIds ids = new EventIds();
        Instant start = Instant.now();
        List<EventEnvelope> events = new ArrayList<>();
        while (generator.hasNext()) {
            events.add(generator.next().toEnvelope(runId, ids.next(), start));
        }
        return events;
    }

    /** The generator's record for the event, keyed by user id (Kafka chooses the partition). */
    public static ProducerRecord<String, byte[]> record(String topic, EventEnvelope event) {
        return SimulationService.toRecord(topic, event, CODEC);
    }

    /** The generator's record for the event, forced onto {@code partition}. */
    public static ProducerRecord<String, byte[]> record(String topic, int partition, EventEnvelope event) {
        ProducerRecord<String, byte[]> keyed = record(topic, event);
        return new ProducerRecord<>(topic, partition, keyed.key(), keyed.value(), keyed.headers());
    }

    public static byte[] encode(EventEnvelope event) {
        return CODEC.encode(event);
    }
}
