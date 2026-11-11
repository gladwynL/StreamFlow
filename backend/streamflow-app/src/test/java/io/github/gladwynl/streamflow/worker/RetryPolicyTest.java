package io.github.gladwynl.streamflow.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.config.StreamflowProperties.Retry;
import io.github.gladwynl.streamflow.processing.FailureCategory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/** The back-off per failure category, as handed to Spring Kafka's DefaultErrorHandler. */
class RetryPolicyTest {

    private static final Retry RETRY =
            new Retry(3, Duration.ofMillis(200), Duration.ofSeconds(2), Duration.ofMillis(500), Duration.ofSeconds(10));

    @Test
    void badRecordsAreNotRetried() {
        assertThat(intervals(WorkerKafkaConfiguration.backOff(FailureCategory.BAD_RECORD, RETRY), 5))
                .isEmpty();
    }

    @Test
    void transientFailuresGetTwoRetriesForThreeAttemptsInTotal() {
        assertThat(intervals(WorkerKafkaConfiguration.backOff(FailureCategory.TRANSIENT, RETRY), 10))
                .containsExactly(200L, 400L);
    }

    @Test
    void infrastructureFailuresAreRetriedWithoutLimitAndCappedBackoff() {
        List<Long> intervals = intervals(WorkerKafkaConfiguration.backOff(FailureCategory.INFRASTRUCTURE, RETRY), 50);

        assertThat(intervals).hasSize(50);
        assertThat(intervals).startsWith(500L, 1_000L, 2_000L, 4_000L, 8_000L, 10_000L);
        assertThat(intervals).allSatisfy(interval -> assertThat(interval).isLessThanOrEqualTo(10_000L));
    }

    /** Intervals until STOP, or at most {@code limit} of them. */
    private static List<Long> intervals(BackOff backOff, int limit) {
        BackOffExecution execution = backOff.start();
        List<Long> intervals = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            long next = execution.nextBackOff();
            if (next == BackOffExecution.STOP) {
                break;
            }
            intervals.add(next);
        }
        return intervals;
    }
}
