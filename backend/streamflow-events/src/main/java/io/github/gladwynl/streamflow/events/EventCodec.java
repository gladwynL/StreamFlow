package io.github.gladwynl.streamflow.events;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Strict JSON encoding of {@link EventEnvelope}. Decoding rejects unknown fields, trailing content, unsupported
 * schema versions and unknown event types, and decodes the payload as the type named by {@code eventType}. Content
 * rules (required fields, ranges, formats) are checked separately by {@link EventValidator}.
 *
 * <p>Thread-safe.
 */
public final class EventCodec {

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    public byte[] encode(EventEnvelope envelope) {
        return mapper.writeValueAsBytes(envelope);
    }

    /** JSON of the payload alone, as stored in the {@code events.payload} column. */
    public String encodePayload(EventPayload payload) {
        return mapper.writeValueAsString(payload);
    }

    /** Deterministic JSON of any record, used for content hashing. */
    public byte[] canonicalJson(Record value) {
        return mapper.writeValueAsBytes(value);
    }

    /**
     * @throws EventDecodingException if the bytes are not a well-formed event of a supported schema version
     */
    public EventEnvelope decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new EventDecodingException("Empty event");
        }
        Wire wire;
        try {
            wire = mapper.readValue(bytes, Wire.class);
        } catch (JacksonException e) {
            throw new EventDecodingException("Malformed event JSON: " + e.getOriginalMessage(), e);
        }
        if (wire == null) {
            throw new EventDecodingException("Event is JSON null");
        }
        if (wire.schemaVersion() == null || wire.schemaVersion() != EventEnvelope.SCHEMA_VERSION) {
            throw new EventDecodingException("Unsupported schemaVersion: " + wire.schemaVersion());
        }
        EventType type = EventType.fromWireName(wire.eventType())
                .orElseThrow(() -> new EventDecodingException("Unknown eventType: " + wire.eventType()));

        EventPayload payload = null;
        if (wire.payload() != null && !wire.payload().isNull()) {
            try {
                payload = mapper.treeToValue(wire.payload(), type.payloadType());
            } catch (JacksonException e) {
                throw new EventDecodingException("Malformed " + type + " payload: " + e.getOriginalMessage(), e);
            }
        }
        return new EventEnvelope(
                wire.schemaVersion(),
                wire.eventId(),
                type,
                wire.occurredAt(),
                wire.source(),
                wire.synthetic(),
                wire.runId(),
                wire.sequence(),
                wire.userId(),
                wire.sessionId(),
                payload);
    }

    /** The envelope as it appears on the wire, before the payload's type is known. */
    private record Wire(
            Integer schemaVersion,
            UUID eventId,
            String eventType,
            Instant occurredAt,
            String source,
            Boolean synthetic,
            UUID runId,
            Long sequence,
            String userId,
            String sessionId,
            JsonNode payload) {}
}
