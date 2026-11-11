package io.github.gladwynl.streamflow.simulation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.events.EventType;
import io.github.gladwynl.streamflow.events.EventValidator;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventContentGeneratorTest {

    private static final EventCodec CODEC = new EventCodec();
    private static final SimulationSpec SPEC = new SimulationSpec(42, 100_000, 1_000, 50_000);

    @Test
    void sameSeedAndSpecProduceIdenticalContentOver100kEvents() {
        String first = ContentHasher.hashOf(SPEC, 100_000, CODEC);
        String second = ContentHasher.hashOf(SPEC, 100_000, CODEC);

        assertThat(first).isEqualTo(second).hasSize(64);
    }

    @Test
    void contentDoesNotDependOnTheRate() {
        // The rate only changes scheduled offsets; compare everything else.
        SimulationSpec slow = new SimulationSpec(42, 1_000, 10, 50_000);
        SimulationSpec fast = new SimulationSpec(42, 1_000, 50_000, 50_000);
        EventContentGenerator a = new EventContentGenerator(slow);
        EventContentGenerator b = new EventContentGenerator(fast);
        while (a.hasNext()) {
            GeneratedEvent x = a.next();
            GeneratedEvent y = b.next();
            assertThat(y.eventType()).isEqualTo(x.eventType());
            assertThat(y.userId()).isEqualTo(x.userId());
            assertThat(y.sessionId()).isEqualTo(x.sessionId());
            assertThat(y.payload()).isEqualTo(x.payload());
        }
    }

    @Test
    void differentSeedsProduceDifferentContent() {
        SimulationSpec other = new SimulationSpec(43, 100_000, 1_000, 50_000);

        assertThat(ContentHasher.hashOf(other, 10_000, CODEC)).isNotEqualTo(ContentHasher.hashOf(SPEC, 10_000, CODEC));
    }

    @Test
    void producesContiguousSequenceNumbersEveryEventTypeAndOnlyValidEvents() {
        EventContentGenerator generator = new EventContentGenerator(new SimulationSpec(7, 20_000, 1_000, 500));
        EventValidator validator = new EventValidator();
        EventIds ids = new EventIds();
        UUID runId = UUID.randomUUID();
        Instant start = Instant.now();
        Map<EventType, Integer> byType = new EnumMap<>(EventType.class);

        long expectedSequence = 0;
        while (generator.hasNext()) {
            GeneratedEvent content = generator.next();
            assertThat(content.sequence()).isEqualTo(expectedSequence++);
            assertThat(content.scheduledOffsetMicros()).isEqualTo(content.sequence() * 1_000);
            EventEnvelope envelope = content.toEnvelope(runId, ids.next(), start);
            assertThat(validator.violations(envelope))
                    .as("event %d", content.sequence())
                    .isEmpty();
            byType.merge(content.eventType(), 1, Integer::sum);
        }

        assertThat(expectedSequence).isEqualTo(20_000);
        assertThat(byType).containsOnlyKeys(EventType.values());
    }

    @Test
    void twoRunsWithTheSameSeedShareContentButNoIdentities() {
        EventIds ids = new EventIds();
        Instant firstStart = Instant.parse("2026-11-10T10:00:00Z");
        Instant secondStart = Instant.parse("2026-11-10T10:05:00Z");
        UUID firstRun = UUID.randomUUID();
        UUID secondRun = UUID.randomUUID();
        EventContentGenerator a = new EventContentGenerator(SPEC);
        EventContentGenerator b = new EventContentGenerator(SPEC);
        Set<UUID> firstIds = new HashSet<>();
        Set<UUID> secondIds = new HashSet<>();

        for (int i = 0; i < 10_000; i++) {
            GeneratedEvent x = a.next();
            GeneratedEvent y = b.next();
            assertThat(y).isEqualTo(x);
            EventEnvelope first = x.toEnvelope(firstRun, ids.next(), firstStart);
            EventEnvelope second = y.toEnvelope(secondRun, ids.next(), secondStart);
            assertThat(second.occurredAt()).isNotEqualTo(first.occurredAt());
            firstIds.add(first.eventId());
            secondIds.add(second.eventId());
        }

        assertThat(firstIds).hasSize(10_000).doesNotContainAnyElementsOf(secondIds);
        assertThat(secondIds).hasSize(10_000);
    }

    @Test
    void specRejectsOutOfRangeValues() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimulationSpec(1, 0, 1, 1));
        assertThatIllegalArgumentException().isThrownBy(() -> new SimulationSpec(1, 1, 0, 1));
        assertThatIllegalArgumentException().isThrownBy(() -> new SimulationSpec(1, 1, 1, 1_000_000));
    }
}
