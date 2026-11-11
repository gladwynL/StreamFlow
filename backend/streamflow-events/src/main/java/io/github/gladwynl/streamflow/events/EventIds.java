package io.github.gladwynl.streamflow.events;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Time-ordered UUIDv7 event identifiers (RFC 9562).
 *
 * <p>Layout: 48-bit Unix milliseconds, version 7, a 12-bit counter, the RFC variant and 62 random bits from a
 * {@link SecureRandom}. Within one millisecond the counter increases, so identifiers from one process are unique and
 * strictly increasing; if the counter overflows, the timestamp is advanced by one millisecond. Identifiers never
 * depend on a simulation seed, so two runs with the same seed never share event ids.
 *
 * <p>Thread-safe.
 */
public final class EventIds {

    private static final int COUNTER_MAX = 0xFFF;
    /** New milliseconds start the counter in the lower half, leaving room to increment. */
    private static final int COUNTER_START_BOUND = 0x800;

    private final SecureRandom random = new SecureRandom();
    private long lastMillis = -1;
    private int counter;

    public synchronized UUID next() {
        return next(System.currentTimeMillis());
    }

    synchronized UUID next(long nowMillis) {
        long millis = Math.max(nowMillis, lastMillis);
        if (millis == lastMillis) {
            counter++;
            if (counter > COUNTER_MAX) {
                millis++;
                counter = random.nextInt(COUNTER_START_BOUND);
            }
        } else {
            counter = random.nextInt(COUNTER_START_BOUND);
        }
        lastMillis = millis;
        long mostSignificant = (millis << 16) | (0x7L << 12) | counter;
        long leastSignificant = (random.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(mostSignificant, leastSignificant);
    }

    public static boolean isVersion7(UUID id) {
        return id.version() == 7 && id.variant() == 2;
    }

    /** The Unix-millisecond timestamp embedded in a version 7 UUID. */
    public static long timestampMillis(UUID id) {
        return id.getMostSignificantBits() >>> 16;
    }
}
