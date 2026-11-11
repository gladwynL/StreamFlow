package io.github.gladwynl.streamflow.api.simulations;

import io.github.gladwynl.streamflow.api.security.OperatorGuard;
import io.github.gladwynl.streamflow.events.EventType;
import io.github.gladwynl.streamflow.kafka.KafkaAdminException;
import io.github.gladwynl.streamflow.persistence.DashboardQueries;
import io.github.gladwynl.streamflow.persistence.DashboardQueries.TypeCount;
import io.github.gladwynl.streamflow.persistence.RunQueries;
import io.github.gladwynl.streamflow.persistence.RunQueries.RunView;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Bounded synthetic simulation runs: history, live progress, and operator actions (start, stop, reconcile). Starting
 * a run is an operator action because it writes to Kafka and PostgreSQL. Every run is synthetic traffic.
 */
@RestController
@RequestMapping("/api/v1/simulations")
@Profile("api")
class SimulationController {

    static final long MAX_EVENTS = 500_000;
    static final int MAX_RATE = 5_000;
    static final int MAX_DURATION_SECONDS = 600;
    static final int MAX_USERS = 100_000;
    static final int DEFAULT_USERS = 10_000;

    private final SimulationManager manager;
    private final RunQueries runs;
    private final DashboardQueries queries;
    private final ReconciliationService reconciliation;
    private final OperatorGuard guard;
    private final JsonMapper json;
    private final SecureRandom seeds = new SecureRandom();

    SimulationController(
            SimulationManager manager,
            RunQueries runs,
            DashboardQueries queries,
            ReconciliationService reconciliation,
            OperatorGuard guard,
            JsonMapper json) {
        this.manager = manager;
        this.runs = runs;
        this.queries = queries;
        this.reconciliation = reconciliation;
        this.guard = guard;
        this.json = json;
    }

    /**
     * Either {@code events} or {@code durationSeconds} sets the run length; with a duration the run publishes
     * {@code ratePerSecond × durationSeconds} events. A missing seed is chosen at random and returned.
     */
    record StartRequest(
            Long seed,
            Long events,
            Integer durationSeconds,
            Integer ratePerSecond,
            Integer users,
            List<String> eventTypes) {}

    record Started(UUID runId, long seed, long events, int ratePerSecond, int users, List<String> eventTypes) {}

    record Progress(
            UUID runId,
            String phase,
            long requestedEvents,
            int ratePerSecond,
            long attempted,
            long acked,
            long failedDefinite,
            long failedAmbiguous,
            Instant startedAt,
            String error) {}

    record RunDetail(RunView run, List<TypeCount> distribution, JsonNode reconciliation) {}

    @GetMapping
    ResponseEntity<List<RunView>> list(
            @RequestParam(required = false) Instant before, @RequestParam(defaultValue = "20") int limit) {
        if (limit < 1 || limit > DashboardQueries.MAX_PAGE) {
            throw badRequest("limit must be between 1 and " + DashboardQueries.MAX_PAGE);
        }
        return noStore(runs.list(before, limit));
    }

    /** The current (or most recent) run started from this api process; 204 if there is none. */
    @GetMapping("/active")
    ResponseEntity<Progress> active() {
        return manager.current()
                .map(run -> noStore(new Progress(
                        run.progress().runId(),
                        run.phase().name(),
                        run.spec().events(),
                        run.spec().ratePerSecond(),
                        run.progress().attempted(),
                        run.progress().acked(),
                        run.progress().failedDefinite(),
                        run.progress().failedAmbiguous(),
                        run.startedAt(),
                        run.error())))
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/{runId}")
    ResponseEntity<RunDetail> get(@PathVariable UUID runId) {
        RunView run = runs.find(runId).orElseThrow(() -> notFound(runId));
        JsonNode report = run.reconciliationJson() == null ? null : json.readTree(run.reconciliationJson());
        return noStore(new RunDetail(run, queries.runDistribution(runId), report));
    }

    @PostMapping
    ResponseEntity<Started> start(
            @RequestHeader(name = OperatorGuard.HEADER, required = false) String key,
            @RequestBody(required = false) StartRequest request)
            throws InterruptedException {
        guard.authorize(key);
        SimulationSpec spec = validate(request);
        UUID runId;
        try {
            runId = manager.start(spec);
        } catch (SimulationManager.RunInProgressException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
        }
        List<String> types = spec.eventTypes().stream().map(Enum::name).sorted().toList();
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new Started(runId, spec.seed(), spec.events(), spec.ratePerSecond(), spec.users(), types));
    }

    @PostMapping("/active/stop")
    ResponseEntity<Void> stop(@RequestHeader(name = OperatorGuard.HEADER, required = false) String key) {
        guard.authorize(key);
        if (!manager.stop()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "No run is publishing");
        }
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{runId}/reconcile")
    ReconciliationReport reconcile(
            @RequestHeader(name = OperatorGuard.HEADER, required = false) String key, @PathVariable UUID runId) {
        guard.authorize(key);
        if (runs.find(runId).isEmpty()) {
            throw notFound(runId);
        }
        boolean publishing = manager.current()
                .filter(run -> runId.equals(run.progress().runId()))
                .filter(run -> run.phase() != SimulationManager.Phase.RECONCILED)
                .isPresent();
        if (publishing) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "Run " + runId + " is still in progress; it is reconciled when it drains");
        }
        try {
            return reconciliation.reconcile(runId);
        } catch (KafkaAdminException e) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "Kafka did not answer the lag query; try again", e);
        }
    }

    SimulationSpec validate(StartRequest request) {
        if (request == null) {
            throw badRequest("A request body is required");
        }
        int rate = request.ratePerSecond() == null ? 0 : request.ratePerSecond();
        if (rate < 1 || rate > MAX_RATE) {
            throw badRequest("ratePerSecond must be between 1 and " + MAX_RATE);
        }
        long events;
        if (request.events() != null && request.durationSeconds() != null) {
            throw badRequest("Give either events or durationSeconds, not both");
        } else if (request.events() != null) {
            events = request.events();
        } else if (request.durationSeconds() != null) {
            if (request.durationSeconds() < 1 || request.durationSeconds() > MAX_DURATION_SECONDS) {
                throw badRequest("durationSeconds must be between 1 and " + MAX_DURATION_SECONDS);
            }
            events = (long) rate * request.durationSeconds();
        } else {
            throw badRequest("events or durationSeconds is required");
        }
        if (events < 1 || events > MAX_EVENTS) {
            throw badRequest("events must be between 1 and " + MAX_EVENTS);
        }
        if (Math.ceilDiv(events, rate) > MAX_DURATION_SECONDS) {
            throw badRequest("The run would take longer than " + MAX_DURATION_SECONDS
                    + " s at this rate; lower events or raise ratePerSecond");
        }
        int users = request.users() == null ? DEFAULT_USERS : request.users();
        if (users < 1 || users > MAX_USERS) {
            throw badRequest("users must be between 1 and " + MAX_USERS);
        }
        Set<EventType> types = EnumSet.noneOf(EventType.class);
        if (request.eventTypes() != null) {
            if (request.eventTypes().size() > EventType.values().length) {
                throw badRequest("Too many event types");
            }
            for (String type : request.eventTypes()) {
                try {
                    types.add(EventType.valueOf(type));
                } catch (IllegalArgumentException | NullPointerException e) {
                    throw badRequest("Unknown event type " + type);
                }
            }
            if (types.isEmpty()) {
                throw badRequest("eventTypes must not be empty; omit it for every type");
            }
        }
        long seed = request.seed() != null ? request.seed() : seeds.nextLong() & Long.MAX_VALUE;
        return new SimulationSpec(seed, events, rate, users, types);
    }

    private static <T> ResponseEntity<T> noStore(T body) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }

    private static ResponseStatusException notFound(UUID runId) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "No run " + runId);
    }
}
