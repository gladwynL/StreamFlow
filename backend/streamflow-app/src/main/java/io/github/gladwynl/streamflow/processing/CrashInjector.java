package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.config.StreamflowProperties.CrashTest.CrashPoint;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Test hook that kills the JVM with {@link Runtime#halt} at a named point of the processing path, after a given
 * number of records. {@code halt} skips shutdown hooks, so neither Spring nor the Kafka client gets a chance to
 * commit anything: it behaves like a process crash.
 *
 * <p>Disabled unless {@code streamflow.crash-test.point} is set. It is configuration-only (no API can enable it) and
 * logs a warning at startup when enabled. Used only by the crash-recovery integration tests.
 */
@Component
public class CrashInjector {

    /** Exit status of a deliberate crash, distinguishable from a real failure. */
    public static final int CRASH_EXIT_CODE = 86;
    /** Prefix of the line printed to standard output immediately before halting. */
    public static final String CRASH_MARKER = "STREAMFLOW_CRASH";

    private static final Logger log = LoggerFactory.getLogger(CrashInjector.class);

    private final CrashPoint point;
    private final long afterRecords;
    private final AtomicLong seen = new AtomicLong();

    @Autowired
    public CrashInjector(StreamflowProperties properties) {
        this(properties.crashTest().point(), properties.crashTest().afterRecords());
    }

    CrashInjector(CrashPoint point, long afterRecords) {
        this.point = point;
        this.afterRecords = afterRecords;
        if (point != null) {
            log.warn("CRASH TEST ENABLED: the JVM will halt at {} on record {}", point, afterRecords);
        }
    }

    public static CrashInjector disabled() {
        return new CrashInjector(null, 0);
    }

    /** Called inside the transaction, after every write and before the commit. */
    void beforeCommit(ConsumerRecord<?, ?> record, EventEnvelope event) {
        maybeCrash(CrashPoint.BEFORE_DB_COMMIT, record, event);
    }

    /** Called after the commit, before the listener returns (and so before any offset commit). */
    void afterCommit(ConsumerRecord<?, ?> record, EventEnvelope event) {
        maybeCrash(CrashPoint.AFTER_DB_COMMIT, record, event);
    }

    private void maybeCrash(CrashPoint at, ConsumerRecord<?, ?> record, EventEnvelope event) {
        if (point != at || seen.incrementAndGet() != afterRecords) {
            return;
        }
        System.out.printf(
                "%s point=%s eventId=%s partition=%d offset=%d%n",
                CRASH_MARKER, at, event.eventId(), record.partition(), record.offset());
        System.out.flush();
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    }
}
