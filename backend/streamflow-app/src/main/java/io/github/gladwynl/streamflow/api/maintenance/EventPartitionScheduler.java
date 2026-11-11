package io.github.gladwynl.streamflow.api.maintenance;

import io.github.gladwynl.streamflow.persistence.PartitionMaintenance;
import jakarta.annotation.PreDestroy;
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
 * Keeps daily {@code events} partitions created a week ahead, hourly. The SQL function serializes concurrent callers
 * with an advisory lock, so several api instances are safe. Dropping expired partitions belongs to the retention
 * jobs of Phase 3.
 */
@Component
@Profile("api")
class EventPartitionScheduler {

    private static final Logger log = LoggerFactory.getLogger(EventPartitionScheduler.class);

    private final PartitionMaintenance partitions;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "event-partitions");
        thread.setDaemon(true);
        return thread;
    });

    EventPartitionScheduler(PartitionMaintenance partitions) {
        this.partitions = partitions;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        scheduler.scheduleWithFixedDelay(this::ensurePartitions, 0, 1, TimeUnit.HOURS);
    }

    private void ensurePartitions() {
        try {
            int created = partitions.ensureEventPartitions();
            if (created > 0) {
                log.info("Created {} daily events partitions", created);
            }
        } catch (DataAccessException e) {
            log.warn("Could not ensure events partitions: {}", e.getMessage());
        } catch (RuntimeException e) {
            log.error("Unexpected failure ensuring events partitions", e);
        }
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
    }
}
