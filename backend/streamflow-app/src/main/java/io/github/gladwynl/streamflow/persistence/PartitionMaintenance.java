package io.github.gladwynl.streamflow.persistence;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Creates the daily partitions of the {@code events} table ahead of time. */
@Repository
public class PartitionMaintenance {

    public static final int DAYS_BACK = 1;
    public static final int DAYS_AHEAD = 7;

    private final JdbcClient jdbc;

    public PartitionMaintenance(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns the number of partitions created. */
    public int ensureEventPartitions() {
        return jdbc.sql("SELECT ensure_event_partitions(:back, :ahead)")
                .param("back", DAYS_BACK)
                .param("ahead", DAYS_AHEAD)
                .query(Integer.class)
                .single();
    }
}
