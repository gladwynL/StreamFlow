package io.github.gladwynl.streamflow.api.status;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

/** The status endpoint reports real dependency failures instead of failing itself. */
class SystemStatusDegradedIT {

    @Nested
    @SpringBootTest(properties = "streamflow.kafka.topics.dead-letter.name=it.status.never-created")
    @AutoConfigureMockMvc
    @ActiveProfiles("api")
    class MissingTopic {

        @DynamicPropertySource
        static void infrastructure(DynamicPropertyRegistry registry) {
            IntegrationTestInfrastructure.registerAll(registry);
        }

        @Autowired
        MockMvcTester mvc;

        @Test
        void reportsTheMissingTopic() {
            MvcTestResult result = mvc.get().uri("/api/v1/system/status").exchange();

            assertThat(result).hasStatusOk();
            assertThat(result).bodyJson().extractingPath("$.status").isEqualTo("DEGRADED");
            assertThat(result)
                    .bodyJson()
                    .extractingPath("$.kafka.topics[1].present")
                    .isEqualTo(false);
            assertThat(result)
                    .bodyJson()
                    .extractingPath("$.issues")
                    .asArray()
                    .contains("Topic it.status.never-created is missing");
        }
    }

    @Nested
    @SpringBootTest(properties = {"spring.kafka.bootstrap-servers=127.0.0.1:1", "streamflow.kafka.admin-timeout=1s"})
    @AutoConfigureMockMvc
    @ActiveProfiles("api")
    class KafkaUnreachable {

        @DynamicPropertySource
        static void infrastructure(DynamicPropertyRegistry registry) {
            // PostgreSQL only: Kafka points at a closed port.
            IntegrationTestInfrastructure.registerPostgres(registry);
        }

        @Autowired
        MockMvcTester mvc;

        @Test
        void reportsKafkaAsUnreachableAndStillReportsTheDatabase() {
            MvcTestResult result = mvc.get().uri("/api/v1/system/status").exchange();

            assertThat(result).hasStatusOk();
            assertThat(result).bodyJson().extractingPath("$.status").isEqualTo("DEGRADED");
            assertThat(result).bodyJson().extractingPath("$.kafka.reachable").isEqualTo(false);
            assertThat(result)
                    .bodyJson()
                    .extractingPath("$.kafka.error")
                    .asString()
                    .isNotBlank();
            assertThat(result).bodyJson().extractingPath("$.database.reachable").isEqualTo(true);
        }
    }
}
