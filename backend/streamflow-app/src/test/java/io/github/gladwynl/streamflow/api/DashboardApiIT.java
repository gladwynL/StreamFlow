package io.github.gladwynl.streamflow.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.api.live.LiveBroadcaster;
import io.github.gladwynl.streamflow.api.security.OperatorGuard;
import io.github.gladwynl.streamflow.persistence.LatencyBuckets;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The Phase 4 API against a real api, a real worker, Kafka and PostgreSQL: a simulation started over HTTP (with
 * operator authorization and bounds), the run's events paged by keyset cursor, analytics, consumer groups, workers,
 * and the live WebSocket feed (cadence, origin check, client limit, a stalled client while events are processed).
 *
 * <p>The database is shared with other test classes, so global aggregates are asserted as lower bounds and per-run
 * values exactly.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DashboardApiIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("dashboard"));
    private static final String KEY = "dashboard-it-key";
    private static final int MAX_CLIENTS = 3;
    private static final int EVENTS = 3_000;
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private static ConfigurableApplicationContext worker;
    private static ConfigurableApplicationContext api;
    private static String base;
    private static UUID runId;

    @BeforeAll
    static void start() {
        worker = IntegrationTestInfrastructure.startWorker(PIPELINE);
        api = IntegrationTestInfrastructure.startApi(
                PIPELINE,
                "--streamflow.api.operator-key=" + KEY,
                "--streamflow.api.max-live-clients=" + MAX_CLIENTS,
                "--streamflow.generate.drain-timeout=120s");
        base = "http://localhost:" + api.getEnvironment().getProperty("local.server.port");
    }

    @AfterAll
    static void stop() {
        if (api != null) {
            api.close();
        }
        if (worker != null) {
            worker.close();
        }
    }

    @Test
    @Order(1)
    void startingASimulationIsAnAuthorizedBoundedOperatorAction() throws Exception {
        String body = """
                {"seed": 7, "events": %d, "ratePerSecond": 3000, "users": 500,
                 "eventTypes": ["PURCHASE_COMPLETED", "PRODUCT_VIEWED", "USER_LOGIN"]}
                """.formatted(EVENTS);
        assertThat(post("/api/v1/simulations", body, null).statusCode()).isEqualTo(401);
        assertThat(post("/api/v1/simulations", body, "wrong-key").statusCode()).isEqualTo(401);

        HttpResponse<String> tooMany = post("/api/v1/simulations", """
                {"events": 500001, "ratePerSecond": 5000}""", KEY);
        assertThat(tooMany.statusCode()).isEqualTo(400);
        assertThat(tooMany.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type -> assertThat(type).startsWith("application/problem+json"));
        assertThat(post("/api/v1/simulations", """
                {"events": 10000, "ratePerSecond": 10}""", KEY).statusCode()).isEqualTo(400);

        HttpResponse<String> started = post("/api/v1/simulations", body, KEY);
        assertThat(started.statusCode()).as(started.body()).isEqualTo(202);
        JsonNode run = JSON.readTree(started.body());
        runId = UUID.fromString(run.get("runId").asString());
        assertThat(run.get("seed").asLong()).isEqualTo(7);

        // No overlapping runs.
        assertThat(post("/api/v1/simulations", body, KEY).statusCode()).isEqualTo(409);

        await().atMost(Duration.ofSeconds(150))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    JsonNode active = getJson("/api/v1/simulations/active");
                    return "RECONCILED".equals(active.get("phase").asString());
                });
        JsonNode active = getJson("/api/v1/simulations/active");
        assertThat(active.get("runId").asString()).isEqualTo(runId.toString());
        assertThat(active.get("acked").asLong()).isEqualTo(EVENTS);
        assertThat(active.get("error").isNull()).isTrue();

        JsonNode detail = getJson("/api/v1/simulations/" + runId);
        assertThat(detail.at("/run/reconciliationResult").asString()).isEqualTo("PASS");
        assertThat(detail.at("/run/processed").asLong()).isEqualTo(EVENTS);
        assertThat(detail.at("/run/acked").asLong()).isEqualTo(EVENTS);
        assertThat(detail.at("/run/eventTypes")
                        .valueStream()
                        .map(JsonNode::asString)
                        .toList())
                .containsExactly("PRODUCT_VIEWED", "PURCHASE_COMPLETED", "USER_LOGIN");
        Set<String> types = new HashSet<>();
        long total = 0;
        for (JsonNode type : detail.get("distribution")) {
            types.add(type.get("eventType").asString());
            total += type.get("events").asLong();
        }
        assertThat(types).isSubsetOf("PRODUCT_VIEWED", "PURCHASE_COMPLETED", "USER_LOGIN");
        assertThat(total).isEqualTo(EVENTS);
        assertThat(detail.at("/reconciliation/result").asString()).isEqualTo("PASS");

        // Nothing is publishing any more; reconciling again is allowed and still passes.
        assertThat(post("/api/v1/simulations/active/stop", "", KEY).statusCode())
                .isEqualTo(409);
        HttpResponse<String> again = post("/api/v1/simulations/" + runId + "/reconcile", "", KEY);
        assertThat(again.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(again.body()).get("result").asString()).isEqualTo("PASS");
        assertThat(post("/api/v1/simulations/" + UUID.randomUUID() + "/reconcile", "", KEY)
                        .statusCode())
                .isEqualTo(404);

        JsonNode history = getJson("/api/v1/simulations?limit=5");
        assertThat(history.valueStream().map(r -> r.get("runId").asString())).contains(runId.toString());
    }

    @Test
    @Order(2)
    void theRunsEventsArePagedByKeysetCursorWithoutGapsOrRepeats() throws Exception {
        Set<String> seen = new HashSet<>();
        String cursor = null;
        Instant previous = null;
        int pages = 0;
        do {
            JsonNode page = getJson(
                    "/api/v1/events?runId=" + runId + "&limit=100" + (cursor == null ? "" : "&cursor=" + cursor));
            for (JsonNode event : page.get("items")) {
                assertThat(seen.add(event.get("eventId").asString())).isTrue();
                Instant ingestedAt = Instant.parse(event.get("ingestedAt").asString());
                if (previous != null) {
                    assertThat(ingestedAt).isBeforeOrEqualTo(previous); // newest first
                }
                previous = ingestedAt;
                assertThat(event.get("latencyMillis").asLong()).isNotNegative();
            }
            cursor = page.get("nextCursor").isNull()
                    ? null
                    : page.get("nextCursor").asString();
            pages++;
        } while (cursor != null && pages < 100);
        assertThat(seen).hasSize(EVENTS);
        assertThat(pages).isEqualTo(EVENTS / 100);

        JsonNode filtered = getJson("/api/v1/events?runId=" + runId + "&type=USER_LOGIN&limit=10");
        assertThat(filtered.get("items")
                        .valueStream()
                        .map(e -> e.get("eventType").asString()))
                .containsOnly("USER_LOGIN");

        String one = seen.iterator().next();
        JsonNode detail = getJson("/api/v1/events/" + one);
        assertThat(detail.at("/event/eventId").asString()).isEqualTo(one);
        assertThat(detail.get("payload").isObject()).isTrue();
        assertThat(detail.get("deadLetters").isEmpty()).isTrue();

        assertThat(get("/api/v1/events/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
        assertThat(get("/api/v1/events?cursor=garbage").statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/events?limit=101").statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/events?type=NOPE").statusCode()).isEqualTo(400);
    }

    @Test
    @Order(3)
    void analyticsKafkaAndWorkersReportMeasuredValues() throws Exception {
        JsonNode series = getJson("/api/v1/analytics/series?range=15m");
        long processed = 0;
        for (JsonNode point : series.get("points")) {
            processed += point.get("processed").asLong();
        }
        assertThat(processed).isGreaterThanOrEqualTo(EVENTS);
        assertThat(series.at("/latency/samples").asLong()).isGreaterThanOrEqualTo(EVENTS);
        assertThat(series.at("/latency/histogram").size()).isEqualTo(LatencyBuckets.BUCKETS);
        assertThat(series.at("/latency/p50Millis").asLong()).isPositive();
        assertThat(get("/api/v1/analytics/series?range=7d").statusCode()).isEqualTo(400);

        JsonNode distribution = getJson("/api/v1/analytics/distribution?range=15m");
        assertThat(distribution.get("types").isEmpty()).isFalse();
        assertThat(get("/api/v1/analytics/distribution?range=1h&runId=" + runId).statusCode())
                .isEqualTo(400);

        JsonNode groups = getJson("/api/v1/kafka/consumer-groups");
        JsonNode processing = groups.get("processing");
        assertThat(processing.get("groupId").asString()).isEqualTo(PIPELINE.consumerGroup());
        assertThat(processing.get("partitions").size()).isEqualTo(Pipeline.PARTITIONS);
        assertThat(processing.get("members").size()).isPositive();
        long logEnd = 0;
        for (JsonNode partition : processing.get("partitions")) {
            assertThat(partition.get("lag").asLong()).isZero();
            assertThat(partition.get("ownerClientId").isNull()).isFalse();
            logEnd += partition.get("logEndOffset").asLong();
        }
        assertThat(logEnd).isEqualTo(EVENTS);
        assertThat(groups.at("/deadLetterIngestion/groupId").asString()).isEqualTo(PIPELINE.ingestorGroup());

        JsonNode workers = getJson("/api/v1/workers");
        assertThat(workers.valueStream()
                        .filter(w -> w.get("live").asBoolean())
                        .mapToLong(w -> w.at("/counters/processed").asLong())
                        .sum())
                .isGreaterThanOrEqualTo(EVENTS);

        JsonNode live = getJson("/api/v1/live");
        assertThat(live.at("/kafka/totalLag").asLong()).isZero();
        assertThat(live.at("/kafka/error").isNull()).isTrue();
        assertThat(live.get("databaseError").isNull()).isTrue();
        assertThat(live.at("/workers/live").asInt()).isPositive();
        assertThat(live.at("/latency/samples").asLong()).isGreaterThanOrEqualTo(EVENTS);
        assertThat(live.get("recentEvents").size()).isPositive();
    }

    @Test
    @Order(4)
    void theLiveFeedPushesAboutOneSnapshotPerSecondAndEnforcesOriginAndClientLimit() throws Exception {
        Collector collector = new Collector();
        WebSocket socket = connect("http://localhost:5173", collector);
        try {
            Thread.sleep(4_500);
            List<JsonNode> snapshots = collector.snapshots();
            assertThat(snapshots).hasSizeBetween(3, 6);
            for (int i = 1; i < snapshots.size(); i++) {
                assertThat(snapshots.get(i).at("/data/sequence").asLong())
                        .isGreaterThan(snapshots.get(i - 1).at("/data/sequence").asLong());
            }
            assertThat(snapshots.getLast().at("/data/kafka/partitions").size()).isEqualTo(Pipeline.PARTITIONS);

            // Only localhost origins may connect.
            assertThatThrownBy(() -> connect("http://evil.example", new Collector()))
                    .isInstanceOf(CompletionException.class);

            // MAX_CLIENTS concurrent clients; the next one is closed with 1013.
            List<WebSocket> others = new ArrayList<>();
            for (int i = 1; i < MAX_CLIENTS; i++) {
                others.add(connect("http://127.0.0.1:3000", new Collector()));
            }
            Collector refused = new Collector();
            connect("http://localhost:3000", refused);
            await().atMost(Duration.ofSeconds(5)).until(() -> refused.closeCode.isDone());
            assertThat(refused.closeCode.get()).isEqualTo(LiveBroadcaster.TOO_MANY_CLIENTS.getCode());
            others.forEach(o -> o.abort());
        } finally {
            socket.abort();
        }
    }

    @Test
    @Order(5)
    void aStalledBrowserNeitherDelaysOtherClientsNorProcessing() throws Exception {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> api.getBean(LiveBroadcaster.class).clientCount() == 0);
        int port = Integer.parseInt(api.getEnvironment().getProperty("local.server.port"));
        // A raw client that completes the handshake and then never reads a byte.
        try (Socket stalled = new Socket("localhost", port)) {
            stalled.setReceiveBufferSize(1024);
            OutputStream out = stalled.getOutputStream();
            out.write(("GET /ws/v1/live HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                            + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
                            + "Origin: http://localhost:5173\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            InputStream in = stalled.getInputStream();
            byte[] status = new byte[12];
            assertThat(in.readNBytes(status, 0, 12)).isEqualTo(12);
            assertThat(new String(status, StandardCharsets.US_ASCII)).isEqualTo("HTTP/1.1 101");

            Collector healthy = new Collector();
            WebSocket socket = connect("http://localhost:5173", healthy);
            try {
                String body = """
                        {"seed": 11, "events": 4000, "ratePerSecond": 2000, "users": 500}""";
                HttpResponse<String> started = post("/api/v1/simulations", body, KEY);
                assertThat(started.statusCode()).as(started.body()).isEqualTo(202);
                UUID second = UUID.fromString(
                        JSON.readTree(started.body()).get("runId").asString());
                await().atMost(Duration.ofSeconds(150))
                        .pollInterval(Duration.ofMillis(500))
                        .until(() -> "RECONCILED"
                                .equals(getJson("/api/v1/simulations/active")
                                        .get("phase")
                                        .asString()));
                JsonNode run = getJson("/api/v1/simulations/" + second);
                assertThat(run.at("/run/reconciliationResult").asString()).isEqualTo("PASS");
                assertThat(run.at("/run/processed").asLong()).isEqualTo(4_000);

                // The healthy client kept receiving about once per second throughout.
                List<JsonNode> snapshots = healthy.snapshots();
                long seconds =
                        Duration.ofNanos(System.nanoTime() - healthy.startedAt).toSeconds();
                assertThat(snapshots.size()).isGreaterThanOrEqualTo((int) (seconds * 0.7));
            } finally {
                socket.abort();
            }
        }
    }

    private static WebSocket connect(String origin, Collector collector) {
        return HTTP.newWebSocketBuilder()
                .header("Origin", origin)
                .connectTimeout(Duration.ofSeconds(5))
                .buildAsync(URI.create(base.replace("http", "ws") + "/ws/v1/live"), collector)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
    }

    private static final class Collector implements WebSocket.Listener {
        final List<String> messages = new CopyOnWriteArrayList<>();
        final CompletableFuture<Integer> closeCode = new CompletableFuture<>();
        final long startedAt = System.nanoTime();
        private final StringBuilder partial = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                messages.add(partial.toString());
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closeCode.complete(statusCode);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            closeCode.complete(-1);
        }

        List<JsonNode> snapshots() {
            return messages.stream().map(JSON::readTree).toList();
        }
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder(URI.create(base + path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode getJson(String path) throws Exception {
        HttpResponse<String> response = get(path);
        assertThat(response.statusCode()).as(path + ": " + response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private static HttpResponse<String> post(String path, String body, String key) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) {
            request.header(OperatorGuard.HEADER, key);
        }
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
