package io.github.gladwynl.streamflow.support;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.kafka.ConsumerLagReader;
import io.github.gladwynl.streamflow.kafka.TopicInspector;
import io.github.gladwynl.streamflow.persistence.ReconciliationQueries;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.SimulationProducers;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
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
                pipeline.consumerGroup(),
                pipeline.eventsSpec(),
                JsonMapper.builder().build(),
                Clock.systemUTC());
    }

    public Producer<String, byte[]> producer() {
        return producer;
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
