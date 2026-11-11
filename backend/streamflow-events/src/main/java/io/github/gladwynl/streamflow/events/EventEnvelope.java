package io.github.gladwynl.streamflow.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Version 1 of the StreamFlow event envelope.
 *
 * <p>Identity ({@code eventId}, {@code runId}, {@code occurredAt}) is unique per simulation run; content (type, user,
 * session, sequence, payload) is reproducible from a seed. Fields are boxed so a malformed event can be decoded far
 * enough to be reported precisely by {@link EventValidator}.
 *
 * @param sequence position of the event within its run, starting at 0 and without gaps
 */
public record EventEnvelope(
        Integer schemaVersion,
        UUID eventId,
        EventType eventType,
        Instant occurredAt,
        String source,
        Boolean synthetic,
        UUID runId,
        Long sequence,
        String userId,
        String sessionId,
        EventPayload payload) {

    public static final int SCHEMA_VERSION = 1;
    public static final String SIMULATOR_SOURCE = "simulator";
}
