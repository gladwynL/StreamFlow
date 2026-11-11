package io.github.gladwynl.streamflow.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.gladwynl.streamflow.kafka.ConsumerLagReader;
import io.github.gladwynl.streamflow.kafka.KafkaAdminException;
import io.github.gladwynl.streamflow.kafka.TopicSpec;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * While waiting for a run to drain, a failed lag query (a Kafka admin timeout under load) means "not drained yet"
 * and is retried; it never counts as drained and never aborts the wait.
 */
class DrainCheckTest {

    private final ConsumerLagReader lag = mock(ConsumerLagReader.class);
    private final ReconciliationService service = new ReconciliationService(
            null,
            null,
            lag,
            new ReconciliationService.Topology(
                    "group",
                    new TopicSpec("events", 6, (short) 1, Map.of()),
                    "ingestor",
                    new TopicSpec("events.dlt", 6, (short) 1, Map.of())),
            Duration.ofDays(7),
            null,
            Clock.systemUTC());

    @Test
    void aFailedLagQueryIsNotDrainedAndTheNextPollCanSucceed() {
        when(lag.totalLag(anyString(), any()))
                .thenThrow(new KafkaAdminException("timeout", null))
                .thenReturn(0L);
        assertThat(service.isDrainedOrUnknown()).isFalse();
        assertThat(service.isDrainedOrUnknown()).isTrue();
    }

    @Test
    void remainingLagIsNotDrained() {
        when(lag.totalLag(anyString(), any())).thenReturn(5L);
        assertThat(service.isDrainedOrUnknown()).isFalse();
    }
}
