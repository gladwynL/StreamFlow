package io.github.gladwynl.streamflow.api.dashboard;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.api.live.LiveSnapshot;
import io.github.gladwynl.streamflow.api.live.LiveSnapshotService;
import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.events.EventType;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector.GroupState;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.persistence.DashboardQueries;
import io.github.gladwynl.streamflow.persistence.DashboardQueries.EventRow;
import io.github.gladwynl.streamflow.persistence.DashboardQueries.MinutePoint;
import io.github.gladwynl.streamflow.persistence.DashboardQueries.TypeCount;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import io.github.gladwynl.streamflow.persistence.LatencyBuckets;
import io.github.gladwynl.streamflow.persistence.WorkerHeartbeat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Read-only dashboard data. Every response is a measurement from PostgreSQL or Kafka; see docs/api.md. */
@RestController
@RequestMapping("/api/v1")
@Profile("api")
class DashboardController {

    /** Analytics time ranges. Minute aggregates are kept for 30 days; the dashboard charts up to one day. */
    enum Range {
        M15("15m", Duration.ofMinutes(15)),
        H1("1h", Duration.ofHours(1)),
        H6("6h", Duration.ofHours(6)),
        H24("24h", Duration.ofHours(24));

        final String label;
        final Duration duration;

        Range(String label, Duration duration) {
            this.label = label;
            this.duration = duration;
        }

        static Range parse(String value) {
            for (Range r : values()) {
                if (r.label.equals(value)) {
                    return r;
                }
            }
            throw badRequest("range must be one of 15m, 1h, 6h, 24h");
        }
    }

    record EventPage(List<EventRow> items, String nextCursor) {}

    record EventDetailView(EventRow event, JsonNode payload, List<DeadLetterRef> deadLetters) {}

    /** A dead letter recorded for the event (for example before it was replayed and processed). */
    record DeadLetterRef(
            long id,
            String failureCategory,
            String errorMessage,
            Integer attempts,
            Instant deadLetteredAt,
            String status,
            int replayCount,
            Instant replayedAt) {

        static DeadLetterRef of(DeadLetterRepository.DeadLetter d) {
            return new DeadLetterRef(
                    d.id(),
                    d.failureCategory(),
                    d.errorMessage(),
                    d.attempts(),
                    d.deadLetteredAt(),
                    d.status().name(),
                    d.replayCount(),
                    d.replayedAt());
        }
    }

    /**
     * @param p50Millis upper bound of the histogram bucket holding the percentile; null when the minute has no
     *     latency samples
     */
    record SeriesPoint(
            Instant minute,
            long processed,
            double processedPerSecond,
            long amountCents,
            long deadLettered,
            long latencySamples,
            Long p50Millis,
            Long p95Millis,
            Long p99Millis) {}

    record LatencySummary(long samples, Long p50Millis, Long p95Millis, Long p99Millis, List<Bucket> histogram) {}

    /** @param upperBoundMillis null for the last (unbounded) bucket */
    record Bucket(Long upperBoundMillis, long count) {}

    record Series(String range, Instant from, Instant to, List<SeriesPoint> points, LatencySummary latency) {}

    record Distribution(String range, UUID runId, List<TypeCount> types) {}

    record ConsumerGroups(GroupState processing, GroupState deadLetterIngestion) {}

    private final LiveSnapshotService live;
    private final DashboardQueries queries;
    private final DeadLetterRepository deadLetters;
    private final HeartbeatRepository heartbeats;
    private final ConsumerGroupInspector groups;
    private final TopicCatalog topics;
    private final StreamflowProperties properties;
    private final JsonMapper json;

    DashboardController(
            LiveSnapshotService live,
            DashboardQueries queries,
            DeadLetterRepository deadLetters,
            HeartbeatRepository heartbeats,
            ConsumerGroupInspector groups,
            TopicCatalog topics,
            StreamflowProperties properties,
            JsonMapper json) {
        this.live = live;
        this.queries = queries;
        this.deadLetters = deadLetters;
        this.heartbeats = heartbeats;
        this.groups = groups;
        this.topics = topics;
        this.properties = properties;
        this.json = json;
    }

    /** The same snapshot the WebSocket feed pushes; used by dashboards to resynchronise after reconnecting. */
    @GetMapping("/live")
    ResponseEntity<LiveSnapshot> live() {
        return noStore(live.snapshot());
    }

    @GetMapping("/events")
    ResponseEntity<EventPage> events(
            @RequestParam(required = false) String type,
            @RequestParam(required = false) UUID runId,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > DashboardQueries.MAX_PAGE) {
            throw badRequest("limit must be between 1 and " + DashboardQueries.MAX_PAGE);
        }
        Cursor after = cursor == null || cursor.isBlank() ? null : Cursor.decode(cursor);
        List<EventRow> rows = queries.events(
                parseType(type),
                runId,
                after == null ? null : after.ingestedAt,
                after == null ? null : after.eventId,
                limit + 1);
        String next = null;
        if (rows.size() > limit) {
            rows = rows.subList(0, limit);
            EventRow last = rows.getLast();
            next = new Cursor(last.ingestedAt(), last.eventId()).encode();
        }
        return noStore(new EventPage(rows, next));
    }

    @GetMapping("/events/{eventId}")
    ResponseEntity<EventDetailView> event(@PathVariable UUID eventId) {
        DashboardQueries.EventDetail detail = queries.event(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No processed event " + eventId));
        return noStore(new EventDetailView(
                detail.event(),
                json.readTree(detail.payloadJson()),
                deadLetters.findByEventId(eventId).stream()
                        .map(DeadLetterRef::of)
                        .toList()));
    }

    @GetMapping("/analytics/series")
    ResponseEntity<Series> series(@RequestParam(defaultValue = "1h") String range) {
        Range r = Range.parse(range);
        long[] total = new long[LatencyBuckets.BUCKETS];
        List<SeriesPoint> points = queries.minuteSeries(r.duration).stream()
                .map(p -> {
                    for (int i = 0; i < total.length; i++) {
                        total[i] += p.latencyBuckets()[i];
                    }
                    return point(p);
                })
                .toList();
        Instant to = Instant.now();
        return noStore(new Series(r.label, to.minus(r.duration), to, points, summary(total)));
    }

    @GetMapping("/analytics/distribution")
    ResponseEntity<Distribution> distribution(
            @RequestParam(required = false) String range, @RequestParam(required = false) UUID runId) {
        if (runId != null) {
            if (range != null) {
                throw badRequest("Give either range or runId, not both");
            }
            return noStore(new Distribution(null, runId, queries.runDistribution(runId)));
        }
        Range r = Range.parse(range == null ? "1h" : range);
        return noStore(new Distribution(r.label, null, queries.distribution(r.duration)));
    }

    @GetMapping("/kafka/consumer-groups")
    ResponseEntity<ConsumerGroups> consumerGroups() {
        try {
            return noStore(new ConsumerGroups(
                    groups.describe(properties.worker().consumerGroup(), topics.events()),
                    groups.describe(properties.deadLetters().ingestorGroup(), topics.deadLetter())));
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka could not be queried", e);
        }
    }

    @GetMapping("/workers")
    ResponseEntity<List<WorkerHeartbeat>> workers() {
        return noStore(heartbeats.findSeenWithin(
                properties.status().workerWindow(), properties.worker().liveThreshold()));
    }

    static SeriesPoint point(MinutePoint p) {
        long samples = 0;
        for (long c : p.latencyBuckets()) {
            samples += c;
        }
        return new SeriesPoint(
                p.minute(),
                p.processed(),
                p.processed() / 60.0,
                p.amountCents(),
                p.deadLettered(),
                samples,
                LatencyBuckets.quantile(p.latencyBuckets(), 0.50),
                LatencyBuckets.quantile(p.latencyBuckets(), 0.95),
                LatencyBuckets.quantile(p.latencyBuckets(), 0.99));
    }

    static LatencySummary summary(long[] counts) {
        long samples = 0;
        List<Bucket> buckets = new java.util.ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            samples += counts[i];
            buckets.add(new Bucket(LatencyBuckets.upperBound(i), counts[i]));
        }
        return new LatencySummary(
                samples,
                LatencyBuckets.quantile(counts, 0.50),
                LatencyBuckets.quantile(counts, 0.95),
                LatencyBuckets.quantile(counts, 0.99),
                buckets);
    }

    private static String parseType(String type) {
        if (type == null || type.isBlank()) {
            return null;
        }
        try {
            return EventType.valueOf(type).name();
        } catch (IllegalArgumentException e) {
            throw badRequest("Unknown event type " + type);
        }
    }

    /** Opaque keyset cursor: the (ingestedAt, eventId) of the last row of a page. */
    record Cursor(Instant ingestedAt, UUID eventId) {
        String encode() {
            return Base64.getUrlEncoder().withoutPadding().encodeToString((ingestedAt + "|" + eventId).getBytes(UTF_8));
        }

        static Cursor decode(String value) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(value), UTF_8).split("\\|", 2);
                return new Cursor(Instant.parse(parts[0]), UUID.fromString(parts[1]));
            } catch (IllegalArgumentException | DateTimeParseException | ArrayIndexOutOfBoundsException e) {
                throw badRequest("Invalid cursor");
            }
        }
    }

    private static <T> ResponseEntity<T> noStore(T body) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
