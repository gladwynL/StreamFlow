package io.github.gladwynl.streamflow.api.status;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventTopics;
import io.github.gladwynl.streamflow.persistence.HeartbeatRepository;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
class SystemStatusIT {

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        IntegrationTestInfrastructure.registerAll(registry);
    }

    @Autowired
    MockMvcTester mvc;

    @Autowired
    HeartbeatRepository heartbeats;

    @Test
    void reportsRealKafkaDatabaseAndWorkerState() {
        UUID workerId = UUID.randomUUID();
        heartbeats.recordHeartbeat(workerId, "it-worker");

        MvcTestResult result = mvc.get().uri("/api/v1/system/status").exchange();

        assertThat(result).hasStatusOk().headers().hasValue("Cache-Control", "no-store");
        assertThat(result).bodyJson().extractingPath("$.status").isEqualTo("OK");
        assertThat(result).bodyJson().extractingPath("$.issues").asArray().isEmpty();
        assertThat(result).bodyJson().extractingPath("$.kafka.reachable").isEqualTo(true);
        assertThat(result)
                .bodyJson()
                .extractingPath("$.kafka.clusterId")
                .asString()
                .isNotBlank();
        assertThat(result).bodyJson().extractingPath("$.kafka.brokerCount").isEqualTo(1);
        assertThat(result).bodyJson().extractingPath("$.kafka.topics[0].name").isEqualTo(EventTopics.EVENTS);
        assertThat(result)
                .bodyJson()
                .extractingPath("$.kafka.topics[0].partitions")
                .isEqualTo(6);
        assertThat(result)
                .bodyJson()
                .extractingPath("$.kafka.topics[0].configDrift")
                .asArray()
                .isEmpty();
        assertThat(result)
                .bodyJson()
                .extractingPath("$.kafka.topics[1].name")
                .isEqualTo(EventTopics.EVENTS_DEAD_LETTER);
        assertThat(result).bodyJson().extractingPath("$.database.reachable").isEqualTo(true);
        assertThat(result)
                .bodyJson()
                .extractingPath("$.database.serverVersion")
                .asString()
                .startsWith("18.");
        assertThat(result).bodyJson().extractingPath("$.database.schemaVersion").isEqualTo("4");
        assertThat(result)
                .bodyJson()
                .extractingPath("$.workers[*].workerId")
                .asArray()
                .contains(workerId.toString());
        assertThat(result).bodyJson().extractingPath("$.checkedAt").asString().matches("\\d{4}-\\d{2}-\\d{2}T.*Z");
    }

    @Test
    void healthEndpointsAreServedOnTheManagementPortOnly() {
        assertThat(mvc.get().uri("/actuator/health").exchange()).hasStatus(404);
    }
}
