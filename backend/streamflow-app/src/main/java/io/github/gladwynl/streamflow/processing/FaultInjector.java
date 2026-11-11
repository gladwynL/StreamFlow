package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.persistence.FaultScenarioRepository;
import io.github.gladwynl.streamflow.persistence.FaultScenarioRepository.Scenario;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Applies active fault-injection scenarios to events being processed. Disabled unless
 * {@code streamflow.fault-injection.enabled=true}; when disabled it never reads the scenario table and never fails
 * an event. Scenarios are refreshed from PostgreSQL every {@code refresh-interval}; each is also checked against its
 * expiry at the moment it is applied.
 */
@Component
public class FaultInjector {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final FaultScenarioRepository scenarios;
    private final boolean enabled;
    private final Clock clock;
    private volatile List<Scenario> active = List.of();
    private final ScheduledExecutorService refresher;

    @Autowired
    public FaultInjector(FaultScenarioRepository scenarios, StreamflowProperties properties) {
        this(
                scenarios,
                properties.faultInjection().enabled(),
                properties.faultInjection().refreshInterval().toMillis(),
                Clock.systemUTC());
    }

    FaultInjector(FaultScenarioRepository scenarios, boolean enabled, long refreshMillis, Clock clock) {
        this.scenarios = scenarios;
        this.enabled = enabled;
        this.clock = clock;
        if (!enabled) {
            this.refresher = null;
            return;
        }
        log.warn("FAULT INJECTION ENABLED: active scenarios in fault_scenarios will fail matching events");
        this.refresher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fault-scenarios");
            thread.setDaemon(true);
            return thread;
        });
        refresh();
        refresher.scheduleWithFixedDelay(this::refresh, refreshMillis, refreshMillis, TimeUnit.MILLISECONDS);
    }

    public static FaultInjector disabled() {
        return new FaultInjector(null, false, 0, Clock.systemUTC());
    }

    /** The scenarios currently applied (empty when disabled). */
    public List<Scenario> activeScenarios() {
        return active.stream()
                .filter(s -> s.expiresAt().isAfter(clock.instant()))
                .toList();
    }

    /**
     * @param deliveryAttempt 1 for the first delivery of the record to this consumer, 2 for the first retry, ...
     * @throws InjectedFault if an active scenario matches the event
     */
    void apply(EventEnvelope event, int deliveryAttempt) {
        if (!enabled) {
            return;
        }
        for (Scenario scenario : activeScenarios()) {
            if (!scenario.eventType().equals(event.eventType().name())) {
                continue;
            }
            switch (scenario.kind()) {
                case REJECT_EVENT_TYPE ->
                    throw new InjectedFault(
                            FailureCategory.BAD_RECORD,
                            "Injected rejection of " + scenario.eventType() + " (scenario " + scenario.id() + ")");
                case TRANSIENT_FAILURES -> {
                    if (deliveryAttempt <= scenario.failuresPerRecord()) {
                        throw new InjectedFault(
                                FailureCategory.TRANSIENT,
                                "Injected transient failure %d of %d (scenario %d)"
                                        .formatted(deliveryAttempt, scenario.failuresPerRecord(), scenario.id()));
                    }
                }
            }
        }
    }

    private void refresh() {
        try {
            active = List.copyOf(scenarios.active());
        } catch (DataAccessException e) {
            log.warn("Could not refresh fault scenarios: {}", e.getMessage());
        } catch (RuntimeException e) {
            log.error("Unexpected failure refreshing fault scenarios", e);
        }
    }

    @PreDestroy
    void stop() {
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }
}
