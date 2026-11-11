package io.github.gladwynl.streamflow.processing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport.Result;
import io.github.gladwynl.streamflow.simulation.EventContentGenerator;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Every event of a run is published twice; each must take effect exactly once. */
class DuplicateDeliveryIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("duplicates"));
    private static final int EVENTS = 1_000;

    @Test
    void eachEventDeliveredTwiceTakesEffectOnce() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            UUID runId = UUID.randomUUID();
            SimulationSpec spec = new SimulationSpec(11, EVENTS, 1_000, 500);
            tools.runs()
                    .create(
                            runId,
                            spec.seed(),
                            EventContentGenerator.CONTENT_VERSION,
                            EVENTS,
                            1_000,
                            500,
                            PIPELINE.eventsTopic());

            List<ProducerRecord<String, byte[]>> records = records(runId, spec);
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int pass = 0; pass < 2; pass++) {
                for (ProducerRecord<String, byte[]> record : records) {
                    sends.add(tools.producer().send(record));
                }
            }
            tools.producer().flush();
            for (Future<RecordMetadata> send : sends) {
                send.get(30, TimeUnit.SECONDS);
            }
            tools.runs().finish(runId, RunStatus.COMPLETED, null, EVENTS, EVENTS, 0, 0);

            ReconciliationReport report;
            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
                report = tools.reconciliation().awaitDrainAndReconcile(runId, Duration.ofSeconds(90));
            }

            assertThat(report.result()).as(report.reasons().toString()).isEqualTo(Result.PASS);
            assertThat(report.processed()).isEqualTo(EVENTS);
            assertThat(report.eventRows()).isEqualTo(EVENTS);
            assertThat(report.runStatsProcessed()).isEqualTo(EVENTS);
            assertThat(report.duplicatesSuppressed()).isEqualTo(EVENTS);
        }
    }

    private static List<ProducerRecord<String, byte[]>> records(UUID runId, SimulationSpec spec) {
        EventContentGenerator generator = new EventContentGenerator(spec);
        EventIds ids = new EventIds();
        EventCodec codec = new EventCodec();
        Instant start = Instant.now();
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        while (generator.hasNext()) {
            EventEnvelope event = generator.next().toEnvelope(runId, ids.next(), start);
            records.add(new ProducerRecord<>(PIPELINE.eventsTopic(), event.userId(), codec.encode(event)));
        }
        return records;
    }
}
