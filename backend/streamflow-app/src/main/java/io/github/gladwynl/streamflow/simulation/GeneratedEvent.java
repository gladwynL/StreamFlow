package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventPayload;
import io.github.gladwynl.streamflow.events.EventType;
import java.time.Instant;
import java.util.UUID;

/**
 * The seed-reproducible content of one event: everything except its identity. Two runs with the same seed, spec and
 * {@link EventContentGenerator#CONTENT_VERSION} produce equal sequences of these records.
 */
public record GeneratedEvent(
        long sequence,
        long scheduledOffsetMicros,
        EventType eventType,
        String userId,
        String sessionId,
        EventPayload payload) {

    /** Stamps this content with a run's identity. {@code occurredAt} = actual run start + scheduled offset. */
    public EventEnvelope toEnvelope(UUID runId, UUID eventId, Instant runStart) {
        return new EventEnvelope(
                EventEnvelope.SCHEMA_VERSION,
                eventId,
                eventType,
                runStart.plusNanos(scheduledOffsetMicros * 1_000L),
                EventEnvelope.SIMULATOR_SOURCE,
                true,
                runId,
                sequence,
                userId,
                sessionId,
                payload);
    }
}
