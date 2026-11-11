package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.persistence.WorkerCounters;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Worker metrics, exposed through Actuator ({@code /actuator/metrics} on the management port):
 *
 * <ul>
 *   <li>{@code streamflow.events.processed}, {@code streamflow.events.duplicates.suppressed}
 *   <li>{@code streamflow.processing.failures{category}}: every failed delivery attempt
 *   <li>{@code streamflow.deadletters.published{category}}, {@code streamflow.deadletters.publish.failures}
 *   <li>{@code streamflow.deadletters.ingested}, {@code streamflow.deadletters.ingested.duplicates},
 *       {@code streamflow.deadletters.ingestion.failures}
 *   <li>{@code streamflow.infrastructure.outage}: 1 while the last attempt failed for infrastructure reasons
 *   <li>{@code streamflow.infrastructure.recoveries}: successful processing after an infrastructure failure
 *   <li>{@code streamflow.consumer.rebalances}, {@code streamflow.consumer.errors}
 * </ul>
 */
@Component
@Profile("worker")
public class WorkerMetrics {

    final Counter processed;
    final Counter duplicates;
    final Counter deadLetterPublishFailures;
    final Counter ingested;
    final Counter ingestedDuplicates;
    final Counter ingestionFailures;
    final Counter recoveries;
    final Counter rebalances;
    final Counter consumerErrors;
    private final Map<FailureCategory, Counter> failures = new EnumMap<>(FailureCategory.class);
    private final Map<FailureCategory, Counter> deadLettered = new EnumMap<>(FailureCategory.class);
    private final AtomicInteger outage = new AtomicInteger();

    public WorkerMetrics(MeterRegistry meters) {
        processed = meters.counter("streamflow.events.processed");
        duplicates = meters.counter("streamflow.events.duplicates.suppressed");
        deadLetterPublishFailures = meters.counter("streamflow.deadletters.publish.failures");
        ingested = meters.counter("streamflow.deadletters.ingested");
        ingestedDuplicates = meters.counter("streamflow.deadletters.ingested.duplicates");
        ingestionFailures = meters.counter("streamflow.deadletters.ingestion.failures");
        recoveries = meters.counter("streamflow.infrastructure.recoveries");
        rebalances = meters.counter("streamflow.consumer.rebalances");
        consumerErrors = meters.counter("streamflow.consumer.errors");
        for (FailureCategory category : FailureCategory.values()) {
            failures.put(category, meters.counter("streamflow.processing.failures", "category", category.name()));
            deadLettered.put(category, meters.counter("streamflow.deadletters.published", "category", category.name()));
        }
        meters.gauge("streamflow.infrastructure.outage", outage);
    }

    void failedAttempt(FailureCategory category) {
        failures.get(category).increment();
        if (category == FailureCategory.INFRASTRUCTURE) {
            outage.set(1);
        }
    }

    void succeeded() {
        if (outage.compareAndSet(1, 0)) {
            recoveries.increment();
        }
    }

    void deadLettered(FailureCategory category) {
        deadLettered.get(category).increment();
    }

    public double failures(FailureCategory category) {
        return failures.get(category).count();
    }

    public double deadLetteredCount(FailureCategory category) {
        return deadLettered.get(category).count();
    }

    public double deadLetterPublishFailures() {
        return deadLetterPublishFailures.count();
    }

    public double recoveries() {
        return recoveries.count();
    }

    public double rebalances() {
        return rebalances.count();
    }

    public double consumerErrors() {
        return consumerErrors.count();
    }

    /** The counters reported with the worker's heartbeat. */
    WorkerCounters snapshot(List<Integer> assignedPartitions) {
        return new WorkerCounters(
                (long) processed.count(),
                (long) duplicates.count(),
                (long) failures(FailureCategory.BAD_RECORD),
                (long) failures(FailureCategory.TRANSIENT),
                (long) failures(FailureCategory.INFRASTRUCTURE),
                (long) (deadLetteredCount(FailureCategory.BAD_RECORD) + deadLetteredCount(FailureCategory.TRANSIENT)),
                (long) deadLetterPublishFailures(),
                (long) rebalances(),
                (long) consumerErrors(),
                inOutage(),
                assignedPartitions);
    }

    public boolean inOutage() {
        return outage.get() == 1;
    }
}
