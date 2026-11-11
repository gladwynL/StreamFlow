package io.github.gladwynl.streamflow.deadletter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.gladwynl.streamflow.api.security.OperatorGuard;
import io.github.gladwynl.streamflow.deadletter.ReplayService.Outcome;
import io.github.gladwynl.streamflow.deadletter.ReplayService.Result;
import io.github.gladwynl.streamflow.events.EventEnvelope;
import io.github.gladwynl.streamflow.events.EventHeaders;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.DeadLetter;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.NewDeadLetter;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.Status;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import io.github.gladwynl.streamflow.support.IntegrationTestInfrastructure;
import io.github.gladwynl.streamflow.support.Pipeline;
import io.github.gladwynl.streamflow.support.PipelineTools;
import io.github.gladwynl.streamflow.support.TestRecords;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Requirement 10: the api JVM is halted after the replayed record is acknowledged by Kafka but before the dead
 * letter is marked REPLAYED. The claim survives the crash; the sweeper releases it; replaying again publishes the
 * same event a second time, and the processing ledger applies it only once.
 */
class ReplayCrashIT {

    private static final Pipeline PIPELINE = IntegrationTestInfrastructure.provision(Pipeline.named("replay-crash"));
    private static final String KEY = "it-operator-key";

    @Test
    void aReplayInterruptedByACrashIsRecoveredWithOneEffect() throws Exception {
        try (PipelineTools tools = new PipelineTools(PIPELINE)) {
            EventEnvelope event = TestRecords.events(UUID.randomUUID(), 37, 1).getFirst();
            long id = storeDeadLetter(tools, event);

            int port = freePort();
            Path log = Path.of("target", "replay-crash-api.log");
            Process api = new ProcessBuilder(IntegrationTestInfrastructure.processCommand(
                            "api",
                            PIPELINE,
                            List.of(
                                    "--server.port=" + port,
                                    "--management.server.port=" + freePort(),
                                    "--streamflow.api.operator-key=" + KEY,
                                    "--streamflow.crash-test.point=AFTER_REPLAY_PUBLISH",
                                    "--streamflow.crash-test.after-records=1")))
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile())
                    .start();
            try {
                HttpClient http = HttpClient.newHttpClient();
                await().atMost(Duration.ofSeconds(90))
                        .ignoreExceptions()
                        .until(() -> http.send(
                                                HttpRequest.newBuilder(URI.create("http://localhost:" + port
                                                                + "/api/v1/dead-letters/" + id))
                                                        .build(),
                                                HttpResponse.BodyHandlers.ofString())
                                        .statusCode()
                                == 200);

                HttpRequest replay = HttpRequest.newBuilder(
                                URI.create("http://localhost:" + port + "/api/v1/dead-letters/replay"))
                        .header("Content-Type", "application/json")
                        .header(OperatorGuard.HEADER, KEY)
                        .POST(HttpRequest.BodyPublishers.ofString("{\"ids\":[" + id + "]}"))
                        .build();
                assertThatThrownBy(() -> http.send(replay, HttpResponse.BodyHandlers.ofString()))
                        .as("the api dies before answering")
                        .isInstanceOf(IOException.class);
                assertThat(api.waitFor(60, TimeUnit.SECONDS)).isTrue();
            } finally {
                api.destroyForcibly();
            }
            assertThat(api.exitValue()).as("exit status; see " + log).isEqualTo(CrashInjector.CRASH_EXIT_CODE);
            assertThat(Files.readString(log)).contains("STREAMFLOW_CRASH point=AFTER_REPLAY_PUBLISH");

            DeadLetter afterCrash = tools.deadLetters().find(id).orElseThrow();
            assertThat(afterCrash.status()).as("the claim survived the crash").isEqualTo(Status.REPLAY_REQUESTED);
            assertThat(afterCrash.replayCount()).isEqualTo(1);

            try (ConfigurableApplicationContext worker = IntegrationTestInfrastructure.startWorker(PIPELINE)) {
                await().atMost(Duration.ofSeconds(60)).until(() -> tools.ledgerRows(event.eventId()) == 1);

                // The sweeper (claim timeout reached) returns the claim to PENDING; the operator replays again.
                assertThat(tools.deadLetters().releaseStaleClaims(Duration.ZERO))
                        .isEqualTo(1);
                assertThat(tools.replay().replay(List.of(id)))
                        .extracting(Result::outcome)
                        .containsExactly(Outcome.REPLAYED);
                await().atMost(Duration.ofSeconds(60)).until(() -> tools.totalLag() == 0);
            }

            assertThat(tools.ledgerRows(event.eventId())).as("one effect").isEqualTo(1);
            assertThat(tools.queries().duplicatesSuppressed(event.runId()))
                    .as("the second publication was suppressed")
                    .isEqualTo(1);
            assertThat(tools.deadLetters().find(id).orElseThrow().status()).isEqualTo(Status.REPLAYED);
            System.out.println("OBSERVED replay crash: claim survived, released by the sweeper, replayed again;"
                    + " 2 publications, 1 effect, 1 suppressed duplicate");
        }
    }

    /** A dead letter as the ingestor would have stored it for a valid event (e.g. after a faulty release). */
    private static long storeDeadLetter(PipelineTools tools, EventEnvelope event) {
        String headers = "{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%d\",\"%s\":\"1\"}"
                .formatted(
                        EventHeaders.RUN_ID,
                        event.runId(),
                        EventHeaders.EVENT_ID,
                        event.eventId(),
                        EventHeaders.SEQUENCE,
                        event.sequence(),
                        EventHeaders.SCHEMA_VERSION);
        tools.deadLetters()
                .insertIfAbsent(new NewDeadLetter(
                        PIPELINE.eventsTopic(),
                        0,
                        0,
                        Instant.now(),
                        0,
                        0,
                        event.eventId(),
                        event.runId(),
                        event.sequence(),
                        event.userId(),
                        TestRecords.encode(event),
                        headers,
                        "BAD_RECORD",
                        "InjectedFault",
                        "test",
                        1,
                        Instant.now()));
        return tools.deadLetters().findByEventId(event.eventId()).getFirst().id();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
