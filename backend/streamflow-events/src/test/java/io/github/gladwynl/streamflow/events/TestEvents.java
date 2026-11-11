package io.github.gladwynl.streamflow.events;

import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import java.time.Instant;
import java.util.UUID;

final class TestEvents {

    static final EventIds IDS = new EventIds();

    private TestEvents() {}

    static EventEnvelope purchase() {
        return new EventEnvelope(
                1,
                IDS.next(),
                EventType.PURCHASE_COMPLETED,
                Instant.parse("2026-11-10T19:00:00.123456Z"),
                "simulator",
                true,
                UUID.randomUUID(),
                42L,
                "u-004211",
                "s-0123456789abcdef",
                new PurchaseCompleted("o-1a2b3c", 4599L, "CAD", 2));
    }
}
