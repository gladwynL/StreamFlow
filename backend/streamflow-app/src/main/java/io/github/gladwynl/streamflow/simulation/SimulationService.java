package io.github.gladwynl.streamflow.simulation;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventHeaders;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.persistence.RunRepository.FailureOutcome;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes one bounded, seeded simulation run and records the outcome of every attempted event.
 *
 * <p>Each attempt ends in exactly one outcome: acknowledged by the broker, failed definitely, or failed
 * ambiguously. Failures are persisted by sequence number; acknowledged events are not stored individually (they are
 * every attempted sequence number without a failure). Before the run is closed the producer is flushed, so every
 * callback has completed. A run whose failures exceed the cap stops early and is marked {@code INCOMPLETE}; a run
 * whose generator process dies stays {@code RUNNING}. Neither can reconcile as PASS.
 *
 * <p>Publication is paced at a constant rate. Each event's {@code occurredAt} is its <em>scheduled</em> time, so a
 * publisher that falls behind shows up as latency instead of being hidden.
 */
public final class SimulationService {

    private static final Logger log = LoggerFactory.getLogger(SimulationService.class);
    private static final int FAILURE_FLUSH_INTERVAL = 1_000;

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final RunRepository runs;
    private final EventCodec codec;
    private final EventIds ids;
    private final long maxPublishFailures;
    private final Clock clock;

    public SimulationService(
            Producer<String, byte[]> producer,
            String topic,
            RunRepository runs,
            EventCodec codec,
            EventIds ids,
            long maxPublishFailures,
            Clock clock) {
        this.producer = producer;
        this.topic = topic;
        this.runs = runs;
        this.codec = codec;
        this.ids = ids;
        this.maxPublishFailures = maxPublishFailures;
        this.clock = clock;
    }

    public RunSummary run(SimulationSpec spec) {
        UUID runId = UUID.randomUUID();
        Instant runStart = clock.instant();
        runs.create(
                runId,
                spec.seed(),
                EventContentGenerator.CONTENT_VERSION,
                spec.events(),
                spec.ratePerSecond(),
                spec.users(),
                topic);
        log.info(
                "Run {} started: seed={}, events={}, rate={}/s, topic={}",
                runId,
                spec.seed(),
                spec.events(),
                spec.ratePerSecond(),
                topic);

        EventContentGenerator generator = new EventContentGenerator(spec);
        ContentHasher hasher = new ContentHasher(codec);
        Counters counters = new Counters();
        Queue<Failure> pendingFailures = new ConcurrentLinkedQueue<>();
        String stopReason = null;
        long startNanos = System.nanoTime();

        while (generator.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                stopReason = "interrupted";
                break;
            }
            if (counters.failures() > maxPublishFailures) {
                stopReason = "more than " + maxPublishFailures + " publish failures";
                break;
            }
            GeneratedEvent content = generator.next();
            hasher.add(content);
            waitUntil(startNanos + content.scheduledOffsetMicros() * 1_000L);
            publish(content.toEnvelope(runId, ids.next(), runStart), counters, pendingFailures);
            if (content.sequence() % FAILURE_FLUSH_INTERVAL == 0) {
                persistFailures(runId, pendingFailures);
            }
        }

        producer.flush();
        persistFailures(runId, pendingFailures);

        RunStatus status = RunStatus.COMPLETED;
        String detail = null;
        if (stopReason != null) {
            status = RunStatus.INCOMPLETE;
            detail = "stopped early: " + stopReason;
        } else if (counters.outcomes() != counters.attempted.get()) {
            status = RunStatus.INCOMPLETE;
            detail = "missing producer outcomes: attempted=" + counters.attempted.get() + ", outcomes="
                    + counters.outcomes();
        }
        runs.finish(
                runId,
                status,
                detail,
                counters.attempted.get(),
                counters.acked.get(),
                counters.definite.get(),
                counters.ambiguous.get());

        RunSummary summary = new RunSummary(
                runId,
                status,
                detail,
                counters.attempted.get(),
                counters.acked.get(),
                counters.definite.get(),
                counters.ambiguous.get(),
                hasher.hex(),
                Duration.ofNanos(System.nanoTime() - startNanos));
        log.info("Run {} finished: {}", runId, summary);
        return summary;
    }

    private void publish(EventEnvelope event, Counters counters, Queue<Failure> failures) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(EventHeaders.RUN_ID, event.runId().toString().getBytes(UTF_8));
        headers.add(EventHeaders.EVENT_ID, event.eventId().toString().getBytes(UTF_8));
        headers.add(EventHeaders.SEQUENCE, Long.toString(event.sequence()).getBytes(UTF_8));
        headers.add(
                EventHeaders.SCHEMA_VERSION,
                Integer.toString(event.schemaVersion()).getBytes(UTF_8));
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, null, event.userId(), codec.encode(event), headers);

        counters.attempted.incrementAndGet();
        try {
            producer.send(record, (metadata, error) -> {
                if (error == null) {
                    counters.acked.incrementAndGet();
                } else {
                    recordFailure(event, error, false, counters, failures);
                }
            });
        } catch (RuntimeException error) {
            recordFailure(event, error, true, counters, failures);
        }
    }

    private static void recordFailure(
            EventEnvelope event, Throwable error, boolean thrownBySend, Counters counters, Queue<Failure> failures) {
        FailureOutcome outcome = PublishFailureClassifier.classify(error, thrownBySend);
        (outcome == FailureOutcome.DEFINITE ? counters.definite : counters.ambiguous).incrementAndGet();
        failures.add(new Failure(event.sequence(), event.eventId(), outcome, error.toString()));
    }

    private void persistFailures(UUID runId, Queue<Failure> failures) {
        Failure failure;
        while ((failure = failures.poll()) != null) {
            runs.recordFailure(runId, failure.sequence(), failure.eventId(), failure.outcome(), failure.error());
        }
    }

    private static void waitUntil(long deadlineNanos) {
        long remaining;
        while ((remaining = deadlineNanos - System.nanoTime()) > 0
                && !Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(remaining);
        }
    }

    private record Failure(long sequence, UUID eventId, FailureOutcome outcome, String error) {}

    private static final class Counters {
        final AtomicLong attempted = new AtomicLong();
        final AtomicLong acked = new AtomicLong();
        final AtomicLong definite = new AtomicLong();
        final AtomicLong ambiguous = new AtomicLong();

        long failures() {
            return definite.get() + ambiguous.get();
        }

        long outcomes() {
            return acked.get() + failures();
        }
    }
}
