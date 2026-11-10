package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import io.github.gladwynl.streamflow.persistence.WorkerHeartbeat;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "streamflow.worker.heartbeat-interval=200ms")
@ActiveProfiles("worker")
class WorkerHeartbeatIT {

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        IntegrationTestInfrastructure.registerAll(registry);
    }

    @Autowired
    HeartbeatPublisher publisher;

    @Autowired
    HeartbeatRepository heartbeats;

    @LocalServerPort
    int port;

    @Test
    void recordsAndRefreshesItsHeartbeat() {
        AtomicReference<Instant> firstSeen = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            WorkerHeartbeat heartbeat = ownHeartbeat().orElseThrow();
            assertThat(heartbeat.live()).isTrue();
            firstSeen.set(heartbeat.lastSeenAt());
        });

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(ownHeartbeat().orElseThrow().lastSeenAt()).isAfter(firstSeen.get()));
    }

    @Test
    void reportsReadyWhenDatabaseAndKafkaAreReachable() {
        ResponseEntity<String> response = RestClient.create()
                .get()
                .uri("http://localhost:" + port + "/actuator/health/readiness")
                .retrieve()
                .toEntity(String.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }

    private Optional<WorkerHeartbeat> ownHeartbeat() {
        return heartbeats.findSeenWithin(Duration.ofMinutes(1), Duration.ofSeconds(5)).stream()
                .filter(h -> h.workerId().equals(publisher.workerId()))
                .findFirst();
    }
}
