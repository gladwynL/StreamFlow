package io.github.gladwynl.streamflow.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.gladwynl.streamflow.events.EventPayload.CartAction;
import io.github.gladwynl.streamflow.events.EventPayload.CartUpdated;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import io.github.gladwynl.streamflow.events.EventPayload.UserLogin;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventValidatorTest {

    private final EventValidator validator =
            new EventValidator(Clock.fixed(Instant.parse("2026-11-10T20:00:00Z"), ZoneOffset.UTC));

    @Test
    void acceptsAWellFormedEvent() {
        assertThat(validator.violations(TestEvents.purchase())).isEmpty();
    }

    @Test
    void reportsEveryEnvelopeViolation() {
        EventEnvelope bad = new EventEnvelope(
                1,
                UUID.randomUUID(), // version 4, not 7
                EventType.PURCHASE_COMPLETED,
                Instant.parse("2026-11-12T20:00:01Z"), // more than a day ahead
                "elsewhere",
                false,
                null,
                -1L,
                "user-1",
                "s-XYZ",
                new PurchaseCompleted("o-1", 4599L, "CAD", 2));

        assertThat(validator.violations(bad))
                .containsExactly(
                        "eventId must be a version 7 UUID",
                        "occurredAt is outside the accepted range",
                        "source must be 'simulator'",
                        "synthetic must be true for simulator events",
                        "runId is required",
                        "sequence must be zero or positive",
                        "userId has an invalid format",
                        "sessionId has an invalid format");
    }

    @Test
    void rejectsPayloadThatDoesNotMatchTheEventType() {
        EventEnvelope mismatched = withPayload(EventType.USER_LOGIN, new PurchaseCompleted("o-1", 4599L, "CAD", 2));

        assertThat(validator.violations(mismatched)).containsExactly("payload does not match eventType USER_LOGIN");
    }

    @Test
    void checksPayloadRanges() {
        assertThat(validator.violations(
                        withPayload(EventType.PURCHASE_COMPLETED, new PurchaseCompleted("o-1", 0L, "cad", 0))))
                .containsExactly(
                        "payload.amountCents must be between 1 and 10000000",
                        "payload.currency has an invalid format",
                        "payload.itemCount must be between 1 and 1000");
        assertThat(validator.violations(
                        withPayload(EventType.CART_UPDATED, new CartUpdated(CartAction.ADD, "p-1", 101))))
                .containsExactly("payload.quantity must be between 1 and 100");
        assertThat(validator.violations(withPayload(EventType.USER_LOGIN, new UserLogin(null, null))))
                .containsExactly("payload.method is required", "payload.success is required");
    }

    @Test
    void requireValidThrowsWithAllViolations() {
        EventEnvelope noPayload = withPayload(EventType.USER_LOGIN, null);

        assertThatThrownBy(() -> validator.requireValid(noPayload))
                .isInstanceOf(EventValidationException.class)
                .hasMessage("Invalid event: payload is required");
    }

    private static EventEnvelope withPayload(EventType type, EventPayload payload) {
        EventEnvelope base = TestEvents.purchase();
        return new EventEnvelope(
                base.schemaVersion(),
                base.eventId(),
                type,
                base.occurredAt(),
                base.source(),
                base.synthetic(),
                base.runId(),
                base.sequence(),
                base.userId(),
                base.sessionId(),
                payload);
    }
}
