package io.github.gladwynl.streamflow.persistence;

import java.util.List;

/**
 * Process-lifetime counters a worker reports with each heartbeat. They restart at zero when the worker process
 * restarts; rates are derived from differences between heartbeats of the same worker.
 *
 * @param processed events applied for the first time
 * @param duplicates deliveries suppressed by the ledger
 * @param failuresBadRecord failed delivery attempts classified as bad records
 * @param failuresTransient failed delivery attempts classified as transient
 * @param failuresInfrastructure failed delivery attempts classified as infrastructure
 * @param deadLettered records published to the dead-letter topic
 * @param deadLetterFailures failed dead-letter publications (the record is redelivered)
 * @param inOutage whether the last attempt failed for infrastructure reasons
 * @param assignedPartitions event-topic partitions currently assigned to this worker
 */
public record WorkerCounters(
        long processed,
        long duplicates,
        long failuresBadRecord,
        long failuresTransient,
        long failuresInfrastructure,
        long deadLettered,
        long deadLetterFailures,
        long rebalances,
        long consumerErrors,
        boolean inOutage,
        List<Integer> assignedPartitions) {

    public static final WorkerCounters NONE = new WorkerCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of());

    public WorkerCounters {
        assignedPartitions = List.copyOf(assignedPartitions);
    }
}
