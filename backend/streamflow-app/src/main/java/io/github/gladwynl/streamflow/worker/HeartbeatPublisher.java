package io.github.gladwynl.streamflow.worker;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import jakarta.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Records this worker's heartbeat at a fixed interval so the API can report which workers are live. A failed
 * heartbeat is logged and retried on the next tick; it never stops the worker.
 */
@Component
@Profile("worker")
class HeartbeatPublisher {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatPublisher.class);
    private static final int MAX_HOST_LENGTH = 255;

    private final HeartbeatRepository repository;
    private final Duration interval;
    private final Duration retention;
    private final UUID workerId = UUID.randomUUID();
    private final String host = resolveHost();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "worker-heartbeat");
        thread.setDaemon(true);
        return thread;
    });

    HeartbeatPublisher(HeartbeatRepository repository, StreamflowProperties properties) {
        this.repository = repository;
        this.interval = properties.worker().heartbeatInterval();
        this.retention = properties.worker().heartbeatRetention();
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        try {
            int purged = repository.deleteSeenBefore(retention);
            log.info(
                    "Worker {} on {} starting heartbeats every {}; purged {} stale heartbeats",
                    workerId,
                    host,
                    interval,
                    purged);
        } catch (DataAccessException e) {
            log.warn("Could not purge stale heartbeats: {}", e.getMessage());
        }
        scheduler.scheduleWithFixedDelay(this::beat, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    UUID workerId() {
        return workerId;
    }

    private void beat() {
        try {
            repository.recordHeartbeat(workerId, host);
        } catch (DataAccessException e) {
            log.warn("Heartbeat failed for worker {}: {}", workerId, e.getMessage());
        } catch (RuntimeException e) {
            // Keep the schedule alive: an uncaught exception would silently cancel future heartbeats.
            log.error("Unexpected heartbeat failure for worker {}", workerId, e);
        }
    }

    @PreDestroy
    void stop() throws InterruptedException {
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
        try {
            repository.delete(workerId);
            log.info("Worker {} stopped; heartbeat removed", workerId);
        } catch (DataAccessException e) {
            log.warn("Could not remove heartbeat of worker {}: {}", workerId, e.getMessage());
        }
    }

    private static String resolveHost() {
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isBlank()) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                host = "unknown";
            }
        }
        return host.length() > MAX_HOST_LENGTH ? host.substring(0, MAX_HOST_LENGTH) : host;
    }
}
