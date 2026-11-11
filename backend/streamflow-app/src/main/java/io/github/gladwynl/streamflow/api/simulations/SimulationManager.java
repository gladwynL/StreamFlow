package io.github.gladwynl.streamflow.api.simulations;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.RunProgress;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Runs simulations started from the dashboard, one at a time. A run is published on a dedicated thread, then the
 * manager waits for the workers to drain it and reconciles it. A new run cannot start until the previous one has
 * been reconciled, so runs never overlap.
 */
@Component
@Profile("api")
public class SimulationManager {

    private static final Logger log = LoggerFactory.getLogger(SimulationManager.class);
    private static final Duration START_TIMEOUT = Duration.ofSeconds(10);

    public enum Phase {
        PUBLISHING,
        DRAINING,
        RECONCILED
    }

    public static final class Active {
        private final RunProgress progress = new RunProgress();
        private final SimulationSpec spec;
        private final Instant startedAt = Instant.now();
        private volatile Phase phase = Phase.PUBLISHING;
        private volatile String error;

        Active(SimulationSpec spec) {
            this.spec = spec;
        }

        public RunProgress progress() {
            return progress;
        }

        public SimulationSpec spec() {
            return spec;
        }

        public Instant startedAt() {
            return startedAt;
        }

        public Phase phase() {
            return phase;
        }

        public String error() {
            return error;
        }
    }

    /** A run is already being published or reconciled. */
    public static class RunInProgressException extends RuntimeException {
        RunInProgressException(UUID runId) {
            super("Run " + runId + " is still in progress; stop it or wait for its reconciliation");
        }
    }

    private final SimulationService simulation;
    private final ReconciliationService reconciliation;
    private final Duration drainTimeout;
    private final AtomicReference<Active> active = new AtomicReference<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "simulation");
        thread.setDaemon(true);
        return thread;
    });

    public SimulationManager(
            SimulationService simulation, ReconciliationService reconciliation, StreamflowProperties properties) {
        this.simulation = simulation;
        this.reconciliation = reconciliation;
        this.drainTimeout = properties.generate().drainTimeout();
    }

    /**
     * Starts a run and returns its id once the run row exists.
     *
     * @throws RunInProgressException if another run has not been reconciled yet
     */
    public UUID start(SimulationSpec spec) throws InterruptedException {
        Active current = active.get();
        if (current != null && current.phase != Phase.RECONCILED) {
            throw new RunInProgressException(current.progress.runId());
        }
        Active next = new Active(spec);
        if (!active.compareAndSet(current, next)) {
            throw new RunInProgressException(null);
        }
        executor.submit(() -> execute(next));
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (next.progress.runId() == null && next.phase == Phase.PUBLISHING && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        if (next.progress.runId() == null) {
            throw new IllegalStateException("The run did not start: " + next.error);
        }
        return next.progress.runId();
    }

    private void execute(Active run) {
        try {
            simulation.run(run.spec, run.progress);
            run.phase = Phase.DRAINING;
            reconciliation.awaitDrainAndReconcile(run.progress.runId(), drainTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            run.error = "interrupted";
        } catch (RuntimeException e) {
            run.error = e.toString();
            log.error("Simulation run {} failed", run.progress.runId(), e);
        } finally {
            run.phase = Phase.RECONCILED;
        }
    }

    /** Requests the publishing run to stop; returns false if no run is publishing. */
    public boolean stop() {
        Active current = active.get();
        if (current == null || current.phase != Phase.PUBLISHING) {
            return false;
        }
        current.progress.requestStop();
        return true;
    }

    /** The current or most recent run of this api process. */
    public Optional<Active> current() {
        return Optional.ofNullable(active.get());
    }

    @PreDestroy
    void shutdown() throws InterruptedException {
        current().ifPresent(run -> run.progress.requestStop());
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
    }
}
