package io.github.gladwynl.streamflow.api.maintenance;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.PartitionMaintenance;
import io.github.gladwynl.streamflow.persistence.RetentionMaintenance;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Periodic maintenance in the api role. Each job's statements are conditional and safe to run concurrently, so
 * several api instances need no coordination (partition DDL is serialized inside PostgreSQL by an advisory lock).
 *
 * <ul>
 *   <li>hourly: create daily {@code events} partitions a week ahead, then apply retention;
 *   <li>every 30 seconds: release replay claims older than the claim timeout (their replayer crashed).
 * </ul>
 */
@Component
@Profile("api")
class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);
    private static final Duration SWEEP_INTERVAL = Duration.ofSeconds(30);

    private final PartitionMaintenance partitions;
    private final RetentionMaintenance retention;
    private final DeadLetterRepository deadLetters;
    private final StreamflowProperties properties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "maintenance");
        thread.setDaemon(true);
        return thread;
    });

    MaintenanceScheduler(
            PartitionMaintenance partitions,
            RetentionMaintenance retention,
            DeadLetterRepository deadLetters,
            StreamflowProperties properties) {
        this.partitions = partitions;
        this.retention = retention;
        this.deadLetters = deadLetters;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        scheduler.scheduleWithFixedDelay(() -> guarded("hourly maintenance", this::hourly), 0, 1, TimeUnit.HOURS);
        scheduler.scheduleWithFixedDelay(
                () -> guarded("replay claim sweep", this::sweep),
                SWEEP_INTERVAL.toSeconds(),
                SWEEP_INTERVAL.toSeconds(),
                TimeUnit.SECONDS);
    }

    private void hourly() {
        int created = partitions.ensureEventPartitions();
        RetentionMaintenance.Result result = retention.run(properties.retention());
        log.info("Maintenance: {} events partitions created, retention {}", created, result);
    }

    private void sweep() {
        int released = deadLetters.releaseStaleClaims(properties.deadLetters().claimTimeout());
        if (released > 0) {
            log.warn("Released {} stale replay claim(s); their dead letters are PENDING again", released);
        }
    }

    private static void guarded(String job, Runnable task) {
        try {
            task.run();
        } catch (RuntimeException e) {
            log.error("{} failed; will retry on the next run", job, e);
        }
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
    }
}
