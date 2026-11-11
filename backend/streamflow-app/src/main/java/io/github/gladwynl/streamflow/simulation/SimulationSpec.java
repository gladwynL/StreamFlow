package io.github.gladwynl.streamflow.simulation;

/**
 * A bounded simulation run.
 *
 * @param seed reproduces the event content; never the event identities
 * @param events number of events to attempt
 * @param ratePerSecond target publication rate (constant)
 * @param users size of the simulated user population
 */
public record SimulationSpec(long seed, long events, int ratePerSecond, int users) {

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
    }

    /** Offset of event {@code sequence} from the run start at the target rate, in microseconds. */
    public long scheduledOffsetMicros(long sequence) {
        return Math.multiplyExact(sequence, 1_000_000L) / ratePerSecond;
    }
}
