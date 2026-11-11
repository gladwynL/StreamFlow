package io.github.gladwynl.streamflow.events;

import io.github.gladwynl.streamflow.events.EventPayload.ApplicationError;
import io.github.gladwynl.streamflow.events.EventPayload.CartUpdated;
import io.github.gladwynl.streamflow.events.EventPayload.PaymentFailed;
import io.github.gladwynl.streamflow.events.EventPayload.ProductViewed;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import io.github.gladwynl.streamflow.events.EventPayload.UserLogin;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Contract rules for version 1 events. Returns every violation, not just the first. Thread-safe. */
public final class EventValidator {

    private static final Pattern USER_ID = Pattern.compile("u-\\d{6}");
    private static final Pattern SESSION_ID = Pattern.compile("s-[0-9a-f]{16}");
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final Instant EARLIEST = Instant.parse("2020-01-01T00:00:00Z");
    private static final Duration MAX_FUTURE_SKEW = Duration.ofDays(1);
    private static final long MAX_AMOUNT_CENTS = 10_000_000L;
    private static final int MAX_QUANTITY = 100;

    private final Clock clock;

    public EventValidator() {
        this(Clock.systemUTC());
    }

    public EventValidator(Clock clock) {
        this.clock = clock;
    }

    public void requireValid(EventEnvelope event) {
        List<String> violations = violations(event);
        if (!violations.isEmpty()) {
            throw new EventValidationException(violations);
        }
    }

    public List<String> violations(EventEnvelope event) {
        List<String> v = new ArrayList<>();
        if (event.schemaVersion() == null || event.schemaVersion() != EventEnvelope.SCHEMA_VERSION) {
            v.add("schemaVersion must be " + EventEnvelope.SCHEMA_VERSION);
        }
        if (event.eventId() == null) {
            v.add("eventId is required");
        } else if (!EventIds.isVersion7(event.eventId())) {
            v.add("eventId must be a version 7 UUID");
        }
        if (event.eventType() == null) {
            v.add("eventType is required");
        }
        if (event.occurredAt() == null) {
            v.add("occurredAt is required");
        } else if (event.occurredAt().isBefore(EARLIEST)
                || event.occurredAt().isAfter(clock.instant().plus(MAX_FUTURE_SKEW))) {
            v.add("occurredAt is outside the accepted range");
        }
        if (!EventEnvelope.SIMULATOR_SOURCE.equals(event.source())) {
            v.add("source must be '" + EventEnvelope.SIMULATOR_SOURCE + "'");
        }
        if (!Boolean.TRUE.equals(event.synthetic())) {
            v.add("synthetic must be true for simulator events");
        }
        if (event.runId() == null) {
            v.add("runId is required");
        }
        if (event.sequence() == null || event.sequence() < 0) {
            v.add("sequence must be zero or positive");
        }
        requireMatch(v, "userId", event.userId(), USER_ID);
        requireMatch(v, "sessionId", event.sessionId(), SESSION_ID);
        validatePayload(v, event);
        return List.copyOf(v);
    }

    private static void validatePayload(List<String> v, EventEnvelope event) {
        EventPayload payload = event.payload();
        if (payload == null) {
            v.add("payload is required");
            return;
        }
        if (event.eventType() != null && !event.eventType().payloadType().isInstance(payload)) {
            v.add("payload does not match eventType " + event.eventType());
            return;
        }
        switch (payload) {
            case UserLogin p -> {
                requireNonNull(v, "payload.method", p.method());
                requireNonNull(v, "payload.success", p.success());
            }
            case ProductViewed p -> {
                requireMatch(v, "payload.productId", p.productId(), IDENTIFIER);
                requireMatch(v, "payload.category", p.category(), IDENTIFIER);
            }
            case CartUpdated p -> {
                requireNonNull(v, "payload.action", p.action());
                requireMatch(v, "payload.productId", p.productId(), IDENTIFIER);
                requireRange(v, "payload.quantity", p.quantity(), 1, MAX_QUANTITY);
            }
            case PurchaseCompleted p -> {
                requireMatch(v, "payload.orderId", p.orderId(), IDENTIFIER);
                requireAmount(v, p.amountCents());
                requireMatch(v, "payload.currency", p.currency(), CURRENCY);
                requireRange(v, "payload.itemCount", p.itemCount(), 1, MAX_QUANTITY * 10);
            }
            case PaymentFailed p -> {
                requireMatch(v, "payload.orderId", p.orderId(), IDENTIFIER);
                requireAmount(v, p.amountCents());
                requireMatch(v, "payload.currency", p.currency(), CURRENCY);
                requireMatch(v, "payload.reasonCode", p.reasonCode(), IDENTIFIER);
            }
            case ApplicationError p -> {
                requireMatch(v, "payload.service", p.service(), IDENTIFIER);
                requireMatch(v, "payload.errorCode", p.errorCode(), IDENTIFIER);
                requireNonNull(v, "payload.severity", p.severity());
            }
        }
    }

    private static void requireNonNull(List<String> v, String field, Object value) {
        if (value == null) {
            v.add(field + " is required");
        }
    }

    private static void requireMatch(List<String> v, String field, String value, Pattern pattern) {
        if (value == null) {
            v.add(field + " is required");
        } else if (!pattern.matcher(value).matches()) {
            v.add(field + " has an invalid format");
        }
    }

    private static void requireRange(List<String> v, String field, Integer value, int min, int max) {
        if (value == null) {
            v.add(field + " is required");
        } else if (value < min || value > max) {
            v.add(field + " must be between " + min + " and " + max);
        }
    }

    private static void requireAmount(List<String> v, Long amountCents) {
        if (amountCents == null) {
            v.add("payload.amountCents is required");
        } else if (amountCents < 1 || amountCents > MAX_AMOUNT_CENTS) {
            v.add("payload.amountCents must be between 1 and " + MAX_AMOUNT_CENTS);
        }
    }
}
