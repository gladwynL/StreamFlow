package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.deadletter.ReplayService;
import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.kafka.ConsumerLagReader;
import io.github.gladwynl.streamflow.kafka.TopicInspector;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.FaultScenarioRepository;
import io.github.gladwynl.streamflow.persistence.ReconciliationQueries;
import io.github.gladwynl.streamflow.persistence.RetentionMaintenance;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.SimulationProducers;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.json.JsonMapper;

/**
 * The production simulation and reconciliation services wired by hand against the test containers, for tests that
 * must publish without a running {@code generate} role (for example, before a worker starts). Close it after use.
 */
public final class PipelineTools implements AutoCloseable {

    private final Pipeline pipeline;
    private final Admin admin = IntegrationTestInfrastructure.newAdmin();
    private final Producer<String, byte[]> producer =
            SimulationProducers.create(IntegrationTestInfrastructure.bootstrapServers(), "it-producer");
    private final JdbcClient jdbc = IntegrationTestInfrastructure.jdbc();
    private final RunRepository runs = new RunRepository(jdbc);
    private final ReconciliationQueries queries = new ReconciliationQueries(jdbc);
    private final DeadLetterRepository deadLetters = new DeadLetterRepository(jdbc);
    private final FaultScenarioRepository faultScenarios = new FaultScenarioRepository(jdbc);
    private final TopicInspector inspector = new TopicInspector(admin, IntegrationTestInfrastructure.adminTimeout());
    private final ConsumerLagReader lagReader = new ConsumerLagReader(admin, inspector);

    public PipelineTools(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public SimulationService simulation() {
        return new SimulationService(
                producer, pipeline.eventsTopic(), runs, new EventCodec(), new EventIds(), 1_000, Clock.systemUTC());
    }

    public ReconciliationService reconciliation() {
        return new ReconciliationService(
                runs,
                queries,
                lagReader,
                new ReconciliationService.Topology(
                        pipeline.consumerGroup(),
                        pipeline.eventsSpec(),
                        pipeline.ingestorGroup(),
                        pipeline.deadLetterSpec()),
                Duration.ofDays(7),
                JsonMapper.builder().build(),
                Clock.systemUTC());
    }

    /** The production replay service with the default seven-day window. */
    public ReplayService replay() {
        return new ReplayService(deadLetters, producer, Duration.ofDays(7), CrashInjector.disabled());
    }

    public DeadLetterRepository deadLetters() {
        return deadLetters;
    }

    public FaultScenarioRepository faultScenarios() {
        return faultScenarios;
    }

    public RetentionMaintenance retention() {
        return new RetentionMaintenance(jdbc, deadLetters);
    }

    public Producer<String, byte[]> producer() {
        return producer;
    }

    /** Number of records written to the pipeline's dead-letter topic (sum of end offsets). */
    public long deadLetterTopicRecords() throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (int p = 0; p < Pipeline.PARTITIONS; p++) {
            request.put(new TopicPartition(pipeline.deadLetterTopic(), p), OffsetSpec.latest());
        }
        return admin.listOffsets(request).all().get(10, TimeUnit.SECONDS).values().stream()
                .mapToLong(ListOffsetsResultInfo::offset)
                .sum();
    }

    public Admin admin() {
        return admin;
    }

    public RunRepository runs() {
        return runs;
    }

    public ReconciliationQueries queries() {
        return queries;
    }

    public JdbcClient jdbc() {
        return jdbc;
    }

    public long totalLag() {
        return lagReader.totalLag(pipeline.consumerGroup(), pipeline.eventsSpec());
    }

    public long lag(String group, io.github.gladwynl.streamflow.kafka.TopicSpec topic) {
        return lagReader.totalLag(group, topic);
    }

    /** Committed offset of the pipeline's group per partition (absent if none). */
    public Map<Integer, Long> committedOffsets() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(pipeline.consumerGroup())
                .partitionsToOffsetAndMetadata()
                .get();
        Map<Integer, Long> offsets = new HashMap<>();
        committed.forEach((tp, offset) -> {
            if (offset != null && tp.topic().equals(pipeline.eventsTopic())) {
                offsets.put(tp.partition(), offset.offset());
            }
        });
        return offsets;
    }

    public long ledgerRows(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = :id")
                .param("id", eventId)
                .query(Long.class)
                .single();
    }

    public List<Long> processedSequences(UUID runId) {
        return jdbc.sql("SELECT run_seq FROM processed_events WHERE run_id = :runId ORDER BY run_seq")
                .param("runId", runId)
                .query(Long.class)
                .list();
    }

    @Override
    public void close() {
        producer.close();
        admin.close();
    }
}
