package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.events.EventType;
import java.util.EnumSet;
import java.util.Set;

/**
 * A bounded simulation run.
 *
 * @param seed reproduces the event content; never the event identities
 * @param events number of events to attempt
 * @param ratePerSecond target publication rate (constant)
 * @param users size of the simulated user population
 * @param hotUserPercent share of draws (0-100) that go to one hot user, {@code u-000000}, to skew keys and
 *     therefore partitions; 0 (the default) draws users uniformly and leaves the content unchanged
 * @param eventTypes event types to emit; the generator's session model still runs in full, and events of other
 *     types are generated and dropped, so content stays a deterministic function of the whole spec
 */
public record SimulationSpec(
        long seed, long events, int ratePerSecond, int users, Set<EventType> eventTypes, int hotUserPercent) {

    public static final long MAX_EVENTS = 10_000_000L;
    public static final int MAX_RATE_PER_SECOND = 100_000;
    public static final int MAX_USERS = 999_999;

    public SimulationSpec {
        if (events < 1 || events > MAX_EVENTS) {
            throw new IllegalArgumentException("events must be between 1 and " + MAX_EVENTS);
        }
        if (ratePerSecond < 1 || ratePerSecond > MAX_RATE_PER_SECOND) {
            throw new IllegalArgumentException("ratePerSecond must be between 1 and " + MAX_RATE_PER_SECOND);
        }
        if (users < 1 || users > MAX_USERS) {
            throw new IllegalArgumentException("users must be between 1 and " + MAX_USERS);
        }
        if (hotUserPercent < 0 || hotUserPercent > 100) {
            throw new IllegalArgumentException("hotUserPercent must be between 0 and 100");
        }
        eventTypes = eventTypes == null || eventTypes.isEmpty()
                ? EnumSet.allOf(EventType.class)
                : EnumSet.copyOf(eventTypes);
    }

    /** A spec without a hot user. */
    public SimulationSpec(long seed, long events, int ratePerSecond, int users, Set<EventType> eventTypes) {
        this(seed, events, ratePerSecond, users, eventTypes, 0);
    }

    /** A spec that emits every event type, without a hot user. */
    public SimulationSpec(long seed, long events, int ratePerSecond, int users) {
        this(seed, events, ratePerSecond, users, null, 0);
    }

    public boolean emits(EventType type) {
        return eventTypes.contains(type);
    }

    /** Offset of event {@code sequence} from the run start at the target rate, in microseconds. */
    public long scheduledOffsetMicros(long sequence) {
        return Math.multiplyExact(sequence, 1_000_000L) / ratePerSecond;
    }
}
