package io.github.gladwynl.streamflow.loadgen;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.config.StreamflowProperties.Loadgen;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector;
import io.github.gladwynl.streamflow.kafka.ConsumerGroupInspector.GroupState;
import io.github.gladwynl.streamflow.kafka.TopicCatalog;
import io.github.gladwynl.streamflow.persistence.RunRepository;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.RunProgress;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * The {@code loadgen} benchmark role (docs/benchmarks/methodology.md). One invocation runs one phase of a benchmark
 * and writes its measurements as JSON; the orchestration script ({@code benchmarks/run.sh}) sequences the phases,
 * starts and stops workers, and samples container resources. It never runs inside the api, and it only targets the
 * topics, consumer groups and database it is configured with (the benchmark stack).
 *
 * <ul>
 *   <li>PUBLISH: publishes one seeded run at the offered rate and records acknowledged throughput and producer
 *       metrics. Every event is accounted for by the run's producer outcomes.
 *   <li>OBSERVE: samples consumer lag and progress until both consumer groups have drained the run, then reconciles
 *       it and records consumer throughput, exact latency percentiles, per-partition distribution and PostgreSQL
 *       statistics. Exit status 0 only if the run reconciles as PASS.
 *   <li>DASHBOARD: holds live-feed WebSocket connections open, like dashboard tabs, for a fixed time.
 * </ul>
 */
@Component
@Profile("loadgen")
class LoadgenRunner implements ApplicationRunner, ExitCodeGenerator {

    private static final Logger log = LoggerFactory.getLogger(LoadgenRunner.class);
    private static final List<String> PRODUCER_METRICS = List.of(
            "record-send-rate",
            "record-size-avg",
            "batch-size-avg",
            "records-per-request-avg",
            "compression-rate-avg",
            "request-latency-avg",
            "request-latency-max",
            "record-retry-total",
            "record-error-total",
            "buffer-exhausted-total");

    private final Loadgen settings;
    private final StreamflowProperties properties;
    private final SimulationService simulation;
    private final ReconciliationService reconciliation;
    private final ConsumerGroupInspector groups;
    private final TopicCatalog topics;
    private final Producer<String, byte[]> producer;
    private final BenchmarkQueries queries;
    private final RunRepository runs;
    private final JsonMapper json =
            JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();
    private volatile int exitCode = 1;

    LoadgenRunner(
            StreamflowProperties properties,
            SimulationService simulation,
            ReconciliationService reconciliation,
            ConsumerGroupInspector groups,
            TopicCatalog topics,
            Producer<String, byte[]> eventProducer,
            RunRepository runs,
            JdbcClient jdbc) {
        this.settings = properties.loadgen();
        this.properties = properties;
        this.simulation = simulation;
        this.reconciliation = reconciliation;
        this.groups = groups;
        this.topics = topics;
        this.producer = eventProducer;
        this.queries = new BenchmarkQueries(jdbc);
        this.runs = runs;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("phase", settings.phase().name());
        result.put("scenario", settings.scenario());
        result.put("environment", environment());
        switch (settings.phase()) {
            case PUBLISH -> publish(result);
            case OBSERVE -> observe(result);
            case DASHBOARD -> dashboard(result);
        }
        Path file = Path.of(settings.resultFile());
        Files.createDirectories(file.toAbsolutePath().getParent());
        Files.writeString(file, json.writeValueAsString(result));
        log.info("Wrote {} result to {}", settings.phase(), file);
    }

    private void publish(Map<String, Object> result) {
        SimulationSpec spec = new SimulationSpec(
                settings.seed(),
                settings.events(),
                settings.ratePerSecond(),
                settings.users(),
                null,
                settings.hotUserPercent());
        result.put(
                "spec",
                Map.of(
                        "seed", spec.seed(),
                        "events", spec.events(),
                        "offeredRatePerSecond", spec.ratePerSecond(),
                        "users", spec.users(),
                        "hotUserPercent", spec.hotUserPercent()));
        Instant started = Instant.now();
        RunSummary summary = simulation.run(spec, new RunProgress());
        double seconds = summary.elapsed().toNanos() / 1e9;
        Map<String, Object> published = new LinkedHashMap<>();
        published.put("runId", summary.runId().toString());
        published.put("status", summary.status().name());
        published.put("startedAt", started.toString());
        published.put("elapsedSeconds", seconds);
        published.put("attempted", summary.attempted());
        published.put("acked", summary.acked());
        published.put("failedDefinite", summary.failedDefinite());
        published.put("failedAmbiguous", summary.failedAmbiguous());
        published.put("ackedPerSecond", seconds > 0 ? summary.acked() / seconds : null);
        published.put("contentHash", summary.contentHash());
        result.put("publish", published);
        result.put("producerMetrics", producerMetrics());
        exitCode = summary.acked() == summary.attempted() ? 0 : 4;
        log.info("Published run {}: {} acked in {} s", summary.runId(), summary.acked(), seconds);
    }

    private void observe(Map<String, Object> result) throws InterruptedException {
        UUID runId = UUID.fromString(settings.runId());
        result.put("runId", runId.toString());
        long acked = runs.find(runId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown run " + runId))
                .acked();
        long intervalMillis = settings.sampleInterval().toMillis();
        result.put("observeStartedEpochMs", System.currentTimeMillis());
        long start = System.nanoTime();
        long deadline = start + settings.drainTimeout().toNanos();
        List<Map<String, Object>> timeline = new ArrayList<>();
        boolean drained = false;
        while (System.nanoTime() < deadline) {
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("t", (System.nanoTime() - start) / 1_000_000);
            long[] progress = queries.processedAndDuplicates(runId);
            long deadLetters = queries.deadLetters(runId);
            sample.put("processed", progress[0]);
            sample.put("duplicates", progress[1]);
            sample.put("deadLetters", deadLetters);
            Long lag = null;
            Long dltLag = null;
            try {
                GroupState group = groups.describe(properties.worker().consumerGroup(), topics.events());
                lag = group.totalLag();
                sample.put(
                        "lagByPartition",
                        group.partitions().stream()
                                .map(ConsumerGroupInspector.PartitionState::lag)
                                .toList());
                sample.put("members", group.members().size());
                dltLag = groups.describe(properties.deadLetters().ingestorGroup(), topics.deadLetter())
                        .totalLag();
            } catch (RuntimeException e) {
                sample.put("kafkaError", e.getMessage());
            }
            sample.put("lag", lag);
            timeline.add(sample);
            if (lag != null && lag == 0 && dltLag != null && dltLag == 0 && progress[0] + deadLetters >= acked) {
                drained = true;
                break;
            }
            Thread.sleep(intervalMillis);
        }
        result.put("drained", drained);
        result.put("observedSeconds", (System.nanoTime() - start) / 1e9);
        result.put("timeline", timeline);
        result.put("acked", acked);

        ReconciliationReport report = reconciliation.reconcile(runId);
        Map<String, Object> reconciled = new LinkedHashMap<>();
        reconciled.put("result", report.result().name());
        reconciled.put("reasons", report.reasons());
        reconciled.put("drained", report.drained());
        reconciled.put("attempted", report.attempted());
        reconciled.put("acked", report.acked());
        reconciled.put("processed", report.processed());
        reconciled.put("duplicatesSuppressed", report.duplicatesSuppressed());
        reconciled.put("deadLetterUnresolved", report.deadLetterUnresolved());
        result.put("reconciliation", reconciled);

        result.put("consumer", queries.consumerThroughput(runId));
        result.put("latency", queries.latency(runId));
        result.put("partitions", queries.partitions(runId));
        result.put("topStatements", queries.topStatements(15));
        result.put("database", queries.database());
        result.put("walIo", queries.walIo());
        result.put("tableSizes", queries.tableSizes());
        exitCode = report.result() == ReconciliationReport.Result.PASS ? 0 : 3;
        log.info("Observed run {}: drained={}, reconciliation={}", runId, drained, report.result());
    }

    private void dashboard(Map<String, Object> result) throws InterruptedException {
        HttpClient http = HttpClient.newHttpClient();
        List<AtomicLong> counters = new ArrayList<>();
        List<WebSocket> sockets = new ArrayList<>();
        for (int i = 0; i < settings.dashboardClients(); i++) {
            AtomicLong received = new AtomicLong();
            counters.add(received);
            sockets.add(http.newWebSocketBuilder()
                    .header("Origin", "http://localhost:8080")
                    .buildAsync(URI.create(settings.dashboardUrl()), new WebSocket.Listener() {
                        @Override
                        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
                            if (last) {
                                received.incrementAndGet();
                            }
                            socket.request(1);
                            return null;
                        }
                    })
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join());
        }
        Thread.sleep(settings.dashboardDuration().toMillis());
        sockets.forEach(WebSocket::abort);
        result.put("clients", settings.dashboardClients());
        result.put("durationSeconds", settings.dashboardDuration().toSeconds());
        result.put("snapshotsReceived", counters.stream().map(AtomicLong::get).toList());
        exitCode = 0;
    }

    private Map<String, Object> producerMetrics() {
        Map<String, Object> out = new TreeMap<>();
        for (Map.Entry<MetricName, ? extends Metric> e : producer.metrics().entrySet()) {
            MetricName name = e.getKey();
            if (name.group().equals("producer-metrics") && PRODUCER_METRICS.contains(name.name())) {
                Object value = e.getValue().metricValue();
                out.put(name.name(), value instanceof Double d && (d.isNaN() || d.isInfinite()) ? null : value);
            }
        }
        return out;
    }

    private Map<String, Object> environment() {
        Map<String, Object> env = new LinkedHashMap<>();
        Runtime runtime = Runtime.getRuntime();
        env.put("javaVersion", Runtime.version().toString());
        env.put("jvm", System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"));
        env.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
        env.put("availableProcessors", runtime.availableProcessors());
        env.put("maxHeapBytes", runtime.maxMemory());
        env.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("eventsTopic", topics.events().name());
        env.put("partitions", topics.events().partitions());
        env.put("consumerGroup", properties.worker().consumerGroup());
        try {
            env.put("postgres", queries.settings());
        } catch (RuntimeException e) {
            env.put("postgres", "unavailable: " + e.getMessage());
        }
        Map<String, String> bench = new TreeMap<>();
        System.getenv().forEach((k, v) -> {
            if (k.startsWith("BENCH_")) {
                bench.put(k, v);
            }
        });
        env.put("benchVariables", bench);
        return env;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }
}
