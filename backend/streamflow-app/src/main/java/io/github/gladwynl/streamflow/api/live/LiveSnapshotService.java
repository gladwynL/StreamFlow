package io.github.gladwynl.streamflow.api.live;

import io.github.gladwynl.streamflow.api.simulations.SimulationManager;
import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector.GroupState;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.persistence.DashboardQueries;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import io.github.gladwynl.streamflow.persistence.LatencyBuckets;
import io.github.gladwynl.streamflow.persistence.RunQueries;
import io.github.gladwynl.streamflow.persistence.WorkerCounters;
import io.github.gladwynl.streamflow.persistence.WorkerHeartbeat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Computes the live snapshot. A snapshot is computed at most once per second however many clients ask for it (the
 * WebSocket broadcaster and REST callers share it), and only when someone asks, so an idle dashboard costs nothing.
 *
 * <p>Rates come from differences between successive measurements: the published rate from the event topic's
 * log-end offsets, failure and duplicate rates from the workers' heartbeat counters over a 10-second window. A
 * worker that restarted gets a new id, so its counters are never subtracted across a restart.
 */
@Service
@Profile("api")
public class LiveSnapshotService {

    private static final Logger log = LoggerFactory.getLogger(LiveSnapshotService.class);

    static final Duration MIN_INTERVAL = Duration.ofMillis(900);
    static final Duration RATE_WINDOW = Duration.ofSeconds(10);
    static final Duration LATENCY_WINDOW = Duration.ofMinutes(5);
    static final int RECENT_EVENTS = 20;
    static final int RECENT_DEAD_LETTERS = 10;

    private record CounterSample(long nanos, Map<UUID, WorkerCounters> counters) {}

    private final DashboardQueries queries;
    private final RunQueries runs;
    private final HeartbeatRepository heartbeats;
    private final DeadLetterRepository deadLetters;
    private final ConsumerGroupInspector groups;
    private final TopicCatalog topics;
    private final SimulationManager simulations;
    private final String consumerGroup;
    private final Duration workerWindow;
    private final Duration liveThreshold;

    private final Deque<CounterSample> counterHistory = new ArrayDeque<>();
    private long sequence;
    private LiveSnapshot cached;
    private long cachedAtNanos;
    private Long previousLogEnd;
    private long previousLogEndNanos;

    public LiveSnapshotService(
            DashboardQueries queries,
            RunQueries runs,
            HeartbeatRepository heartbeats,
            DeadLetterRepository deadLetters,
            ConsumerGroupInspector groups,
            TopicCatalog topics,
            SimulationManager simulations,
            StreamflowProperties properties) {
        this.queries = queries;
        this.runs = runs;
        this.heartbeats = heartbeats;
        this.deadLetters = deadLetters;
        this.groups = groups;
        this.topics = topics;
        this.simulations = simulations;
        this.consumerGroup = properties.worker().consumerGroup();
        this.workerWindow = properties.status().workerWindow();
        this.liveThreshold = properties.worker().liveThreshold();
    }

    /** The latest snapshot, recomputed if the cached one is older than {@link #MIN_INTERVAL}. */
    public synchronized LiveSnapshot snapshot() {
        long now = System.nanoTime();
        if (cached != null && now - cachedAtNanos < MIN_INTERVAL.toNanos()) {
            return cached;
        }
        cached = compute(now);
        cachedAtNanos = now;
        return cached;
    }

    private LiveSnapshot compute(long now) {
        LiveSnapshot.Kafka kafka;
        Double publishedPerSecond = null;
        try {
            GroupState group = groups.describe(consumerGroup, topics.events());
            Map<Integer, Long> sizes = partitionSizes();
            kafka = new LiveSnapshot.Kafka(
                    group.state(),
                    group.members().size(),
                    group.totalLag(),
                    group.logEndOffset(),
                    retentionBytes(),
                    group.partitions().stream()
                            .map(p -> new LiveSnapshot.PartitionView(
                                    p.partition(),
                                    p.logEndOffset(),
                                    p.committedOffset(),
                                    p.lag(),
                                    p.ownerClientId(),
                                    sizes.get(p.partition())))
                            .toList(),
                    null);
            publishedPerSecond = publishedRate(group.logEndOffset(), now);
        } catch (RuntimeException e) {
            log.debug("Kafka query for the live snapshot failed", e);
            kafka = new LiveSnapshot.Kafka(null, null, null, null, null, null, message(e));
            previousLogEnd = null;
        }

        try {
            List<WorkerHeartbeat> seen = heartbeats.findSeenWithin(workerWindow, liveThreshold);
            Map<UUID, WorkerCounters> liveCounters = new HashMap<>();
            seen.stream().filter(WorkerHeartbeat::live).forEach(w -> liveCounters.put(w.workerId(), w.counters()));
            CounterSample base = recordCounters(now, liveCounters);

            Map<String, Double> failedPerSecond = null;
            Double duplicatesPerSecond = null;
            if (base != null) {
                double seconds = (now - base.nanos) / 1e9;
                failedPerSecond = new LinkedHashMap<>();
                failedPerSecond.put("BAD_RECORD", rate(base, liveCounters, WorkerCounters::failuresBadRecord, seconds));
                failedPerSecond.put("TRANSIENT", rate(base, liveCounters, WorkerCounters::failuresTransient, seconds));
                failedPerSecond.put(
                        "INFRASTRUCTURE", rate(base, liveCounters, WorkerCounters::failuresInfrastructure, seconds));
                duplicatesPerSecond = rate(base, liveCounters, WorkerCounters::duplicates, seconds);
            }

            long processed10s = queries.processedWithin(RATE_WINDOW);
            LiveSnapshot.Throughput throughput = new LiveSnapshot.Throughput(
                    publishedPerSecond,
                    processed10s / (double) RATE_WINDOW.toSeconds(),
                    queries.processedWithin(Duration.ofMinutes(1)),
                    queries.deadLetteredWithin(Duration.ofMinutes(1)),
                    failedPerSecond,
                    duplicatesPerSecond);

            long[] histogram = queries.latencyHistogram(LATENCY_WINDOW);
            long samples = 0;
            for (long c : histogram) {
                samples += c;
            }
            LiveSnapshot.Latency latency = new LiveSnapshot.Latency(
                    samples,
                    LatencyBuckets.quantile(histogram, 0.50),
                    LatencyBuckets.quantile(histogram, 0.95),
                    LatencyBuckets.quantile(histogram, 0.99));

            List<LiveSnapshot.WorkerView> workerViews = seen.stream()
                    .map(w -> new LiveSnapshot.WorkerView(
                            w.workerId(),
                            w.host(),
                            w.live(),
                            w.lastSeenAt(),
                            w.counters().processed(),
                            w.counters().duplicates(),
                            w.counters().failuresBadRecord()
                                    + w.counters().failuresTransient()
                                    + w.counters().failuresInfrastructure(),
                            w.counters().deadLettered(),
                            w.counters().inOutage(),
                            w.counters().assignedPartitions()))
                    .toList();
            LiveSnapshot.Workers workers = new LiveSnapshot.Workers(
                    (int) seen.stream().filter(WorkerHeartbeat::live).count(),
                    seen.size(),
                    seen.stream().anyMatch(w -> w.live() && w.counters().inOutage()),
                    workerViews);

            Map<String, Long> deadLetterCounts = new LinkedHashMap<>();
            deadLetters.countsByStatus().forEach((status, n) -> deadLetterCounts.put(status.name(), n));

            return new LiveSnapshot(
                    ++sequence,
                    Instant.now(),
                    throughput,
                    latency,
                    kafka,
                    null,
                    workers,
                    deadLetterCounts,
                    activeRun(),
                    queries.recentEvents(null, RECENT_EVENTS),
                    queries.recentDeadLetters(RECENT_DEAD_LETTERS));
        } catch (RuntimeException e) {
            log.debug("Database query for the live snapshot failed", e);
            return new LiveSnapshot(
                    ++sequence,
                    Instant.now(),
                    null,
                    null,
                    kafka,
                    message(e),
                    null,
                    null,
                    activeRunWithoutDb(),
                    null,
                    null);
        }
    }

    private Map<Integer, Long> partitionSizes() {
        try {
            return groups.partitionSizes(topics.events());
        } catch (RuntimeException e) {
            log.debug("Could not read partition sizes: {}", e.getMessage());
            return Map.of();
        }
    }

    private Long retentionBytes() {
        String value = topics.events().configs().get("retention.bytes");
        return value == null ? null : Long.valueOf(value);
    }

    private Double publishedRate(long logEnd, long now) {
        Double rate = null;
        if (previousLogEnd != null && logEnd >= previousLogEnd && now > previousLogEndNanos) {
            rate = (logEnd - previousLogEnd) / ((now - previousLogEndNanos) / 1e9);
        }
        previousLogEnd = logEnd;
        previousLogEndNanos = now;
        return rate;
    }

    /**
     * Adds a sample and returns the newest sample at least {@link #RATE_WINDOW} old (or the oldest kept sample if
     * none is that old yet but one is at least 2 s old); null when no sample is old enough.
     */
    private CounterSample recordCounters(long now, Map<UUID, WorkerCounters> counters) {
        counterHistory.addLast(new CounterSample(now, counters));
        long window = RATE_WINDOW.toNanos();
        // Drop the oldest sample while the next one is itself old enough to serve as the base.
        while (counterHistory.size() > 1
                && now - counterHistory.stream().skip(1).findFirst().orElseThrow().nanos >= window) {
            counterHistory.removeFirst();
        }
        CounterSample base = counterHistory.peekFirst();
        if (base == null || now - base.nanos < Duration.ofSeconds(2).toNanos()) {
            return null;
        }
        return base;
    }

    /** Sum of counter increases of workers present in both samples, per second. */
    private static double rate(
            CounterSample base, Map<UUID, WorkerCounters> current, ToLongFunction<WorkerCounters> counter, double s) {
        long delta = 0;
        for (Map.Entry<UUID, WorkerCounters> entry : current.entrySet()) {
            WorkerCounters before = base.counters.get(entry.getKey());
            if (before != null) {
                delta += Math.max(0, counter.applyAsLong(entry.getValue()) - counter.applyAsLong(before));
            }
        }
        return delta / s;
    }

    private LiveSnapshot.ActiveRun activeRun() {
        return simulations
                .current()
                .map(run -> {
                    UUID runId = run.progress().runId();
                    Long processed = runId == null
                            ? Long.valueOf(0)
                            : runs.find(runId)
                                    .map(RunQueries.RunView::processed)
                                    .orElse(0L);
                    return toView(run, processed);
                })
                .orElse(null);
    }

    private LiveSnapshot.ActiveRun activeRunWithoutDb() {
        return simulations.current().map(run -> toView(run, null)).orElse(null);
    }

    private static LiveSnapshot.ActiveRun toView(SimulationManager.Active run, Long processed) {
        return new LiveSnapshot.ActiveRun(
                run.progress().runId(),
                run.phase().name(),
                run.spec().events(),
                run.spec().ratePerSecond(),
                run.progress().attempted(),
                run.progress().acked(),
                run.progress().failedDefinite() + run.progress().failedAmbiguous(),
                processed,
                run.startedAt());
    }

    private static String message(RuntimeException e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String text = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
        return text.length() > 200 ? text.substring(0, 200) : text;
    }
}
