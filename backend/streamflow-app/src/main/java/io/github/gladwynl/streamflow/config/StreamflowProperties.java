package io.github.gladwynl.streamflow.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Application settings under the {@code streamflow} prefix. */
@ConfigurationProperties("streamflow")
public record StreamflowProperties(
        @DefaultValue Kafka kafka,
        @DefaultValue Worker worker,
        @DefaultValue Migrate migrate,
        @DefaultValue Status status,
        @DefaultValue Generate generate,
        @DefaultValue Retention retention,
        @DefaultValue CrashTest crashTest,
        @DefaultValue FaultInjection faultInjection,
        @DefaultValue DeadLetters deadLetters,
        @DefaultValue Api api,
        @DefaultValue Loadgen loadgen) {

    /**
     * The {@code loadgen} benchmark role (docs/benchmarks). Each invocation runs one phase and writes a JSON result.
     *
     * @param phase PUBLISH (publish one run and record producer results), OBSERVE (wait for the run to drain, then
     *     record consumer, latency, partition and database measurements and reconcile) or DASHBOARD (hold
     *     {@code dashboardClients} live-feed connections open for {@code dashboardDuration})
     * @param ratePerSecond offered rate; 100,000 (the maximum) publishes as fast as the producer can
     * @param hotUserPercent share of events (0-100) for one hot user, to skew partitions
     * @param runId the run to observe (OBSERVE)
     * @param resultFile where the JSON result is written
     */
    public record Loadgen(
            @DefaultValue("PUBLISH") LoadgenPhase phase,
            @DefaultValue("adhoc") String scenario,
            @DefaultValue("1") long seed,
            @DefaultValue("60000") long events,
            @DefaultValue("100000") int ratePerSecond,
            @DefaultValue("10000") int users,
            @DefaultValue("0") int hotUserPercent,
            String runId,
            @DefaultValue("/results/result.json") String resultFile,
            @DefaultValue("900s") Duration drainTimeout,
            @DefaultValue("1s") Duration sampleInterval,
            @DefaultValue("0") int dashboardClients,
            @DefaultValue("ws://api:8080/ws/v1/live") String dashboardUrl,
            @DefaultValue("120s") Duration dashboardDuration) {}

    /**
     * BATCH (the default since Phase 5): one transaction per poll batch with a record-by-record fallback on failure.
     * RECORD: one database transaction per record, kept as the reference mode. See ADR-0013 and docs/benchmarks.
     */
    public enum ProcessingMode {
        RECORD,
        BATCH
    }

    public enum LoadgenPhase {
        PUBLISH,
        OBSERVE,
        DASHBOARD
    }

    /**
     * Fault-injection scenarios (table {@code fault_scenarios}); see {@code FaultInjector}.
     *
     * @param enabled scenarios are ignored unless this is true; off by default
     */
    public record FaultInjection(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("500ms") Duration refreshInterval) {}

    /**
     * Dead-letter ingestion and replay.
     *
     * @param ingestorGroup consumer group that copies the dead-letter topic into PostgreSQL
     * @param claimTimeout a replay claim older than this is released by the sweeper (the replayer is presumed
     *     crashed); a second publication of the same event is suppressed by the ledger
     * @param maxReplayBatch upper bound on dead letters per replay request
     */
    public record DeadLetters(
            @DefaultValue("streamflow-dlt-ingestor") String ingestorGroup,
            @DefaultValue("2m") Duration claimTimeout,
            @DefaultValue("100") int maxReplayBatch) {}

    /**
     * @param operatorKey required in {@code X-StreamFlow-Operator-Key} for replay and discard; when blank those
     *     operations are disabled
     * @param operatorRequestsPerMinute rate limit of operator requests per api process
     * @param maxLiveClients concurrent WebSocket clients of the live feed; further connections are refused
     * @param liveSendTimeout a client whose snapshot send takes longer than this is disconnected
     * @param liveMaxSkippedTicks consecutive snapshots a client may miss because its previous send is still in
     *     progress before it is disconnected
     */
    public record Api(
            String operatorKey,
            @DefaultValue("30") int operatorRequestsPerMinute,
            @DefaultValue("20") int maxLiveClients,
            @DefaultValue("3s") Duration liveSendTimeout,
            @DefaultValue("5") int liveMaxSkippedTicks) {}

    /**
     * @param adminTimeout upper bound for each Kafka admin request (cluster description, topic description, creation)
     * @param topics the topics StreamFlow owns; see {@code application.yaml}
     */
    public record Kafka(
            @DefaultValue("5s") Duration adminTimeout,
            @DefaultValue Topics topics) {}

    public record Topics(TopicSettings events, TopicSettings deadLetter) {}

    /**
     * @param name optional override of the contract topic name (used by tests to provision isolated topics)
     * @param configs topic-level Kafka configs that the migrate role owns and keeps in sync
     */
    public record TopicSettings(String name, int partitions, short replicationFactor, Map<String, String> configs) {}

    /**
     * @param heartbeatInterval how often each worker records a heartbeat
     * @param heartbeatRetention heartbeats older than this are deleted when a worker starts
     * @param consumerGroup Kafka consumer group of the event processors
     * @param concurrency consumer threads per worker process; workers × concurrency beyond the partition count idle
     * @param maxPollRecords upper bound on records per poll, and so on records redelivered after a crash
     * @param retry in-place retry policy (ADR-0006)
     */
    public record Worker(
            @DefaultValue("2s") Duration heartbeatInterval,
            @DefaultValue("24h") Duration heartbeatRetention,
            @DefaultValue("streamflow-processor") String consumerGroup,
            @DefaultValue("3") int concurrency,
            @DefaultValue("500") int maxPollRecords,
            @DefaultValue Retry retry,
            @DefaultValue("BATCH") ProcessingMode processingMode) {

        /** A worker counts as live if its last heartbeat is newer than three heartbeat intervals. */
        public Duration liveThreshold() {
            return heartbeatInterval.multipliedBy(3);
        }
    }

    /**
     * In-place retries. Attempt counts are kept in memory per partition assignment: a worker restart or rebalance
     * during retries starts the count again (ADR-0006).
     *
     * @param transientMaxAttempts total delivery attempts of a transient failure before it is dead-lettered
     * @param transientInitialBackoff back-off before the first retry; doubles per retry up to the maximum
     * @param infrastructureMaxBackoff cap of the unlimited back-off used while a dependency is unavailable
     */
    public record Retry(
            @DefaultValue("3") int transientMaxAttempts,
            @DefaultValue("200ms") Duration transientInitialBackoff,
            @DefaultValue("2s") Duration transientMaxBackoff,
            @DefaultValue("500ms") Duration infrastructureInitialBackoff,
            @DefaultValue("10s") Duration infrastructureMaxBackoff) {

        public Retry {
            if (transientMaxAttempts < 1 || transientMaxAttempts > 10) {
                throw new IllegalArgumentException("transientMaxAttempts must be between 1 and 10");
            }
        }
    }

    /** @param kafkaReadyTimeout how long the migrate role waits for Kafka to answer before failing */
    public record Migrate(@DefaultValue("60s") Duration kafkaReadyTimeout) {}

    /** @param workerWindow how far back the status endpoint lists workers, live or not */
    public record Status(@DefaultValue("10m") Duration workerWindow) {}

    /**
     * One bounded simulation run of the {@code generate} role.
     *
     * @param drainTimeout how long to wait for the workers to process the run before reconciling anyway
     * @param maxPublishFailures the run stops early, as INCOMPLETE, once failures exceed this
     */
    public record Generate(
            @DefaultValue("42") long seed,
            @DefaultValue("10000") long events,
            @DefaultValue("1000") int ratePerSecond,
            @DefaultValue("50000") int users,
            @DefaultValue("120s") Duration drainTimeout,
            @DefaultValue("10000") long maxPublishFailures) {}

    /**
     * Retention of the deduplication ledger and the windows in which an event can come back (ADR-0008).
     *
     * @param ledger how long ledger rows are kept; must cover every redelivery and replay window plus the margin
     * @param replayWindow dead letters older than this cannot be replayed and become EXPIRED
     * @param events daily {@code events} partitions older than this are dropped
     * @param stats minute aggregates older than this are deleted
     * @param resolvedDeadLetters replayed and discarded dead letters are deleted this long after resolution;
     *     unresolved (pending or expired) dead letters are never deleted automatically
     */
    public record Retention(
            @DefaultValue("8d") Duration ledger,
            @DefaultValue("7d") Duration replayWindow,
            @DefaultValue("1d") Duration margin,
            @DefaultValue("7d") Duration events,
            @DefaultValue("30d") Duration stats,
            @DefaultValue("14d") Duration resolvedDeadLetters) {}

    /**
     * Crash-recovery test hook; see {@code CrashInjector}. Disabled when {@code point} is unset.
     *
     * @param afterRecords halt when this many records have reached {@code point}
     */
    public record CrashTest(
            CrashPoint point, @DefaultValue("0") long afterRecords) {

        public enum CrashPoint {
            BEFORE_DB_COMMIT,
            AFTER_DB_COMMIT,
            /** After the dead-letter record is acknowledged, before the source offset is committed. */
            AFTER_DLT_PUBLISH,
            /** After a replayed record is acknowledged, before the dead letter is marked REPLAYED. */
            AFTER_REPLAY_PUBLISH
        }
    }
}
