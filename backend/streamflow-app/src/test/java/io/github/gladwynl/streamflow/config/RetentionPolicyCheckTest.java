package io.github.gladwynl.streamflow.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import io.github.gladwynl.streamflow.config.StreamflowProperties.Retention;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RetentionPolicyCheckTest {

    private static final Map<String, Duration> TOPICS =
            Map.of("streamflow.events.v1", Duration.ofHours(24), "streamflow.events.v1.dlt", Duration.ofDays(7));

    @Test
    void acceptsTheDefaultPolicy() {
        Retention defaults = new Retention(Duration.ofDays(8), Duration.ofDays(7), Duration.ofDays(1));

        assertThatCode(() -> RetentionPolicyCheck.verify(defaults, TOPICS)).doesNotThrowAnyException();
    }

    @Test
    void rejectsALedgerShorterThanTheLongestTopicRetentionPlusMargin() {
        Retention tooShort = new Retention(Duration.ofDays(7), Duration.ofDays(2), Duration.ofDays(1));

        assertThatIllegalStateException()
                .isThrownBy(() -> RetentionPolicyCheck.verify(tooShort, TOPICS))
                .withMessageContaining("retention of topic streamflow.events.v1.dlt");
    }

    @Test
    void rejectsALedgerShorterThanTheReplayWindowPlusMargin() {
        Retention longReplay = new Retention(Duration.ofDays(8), Duration.ofDays(10), Duration.ofDays(1));

        assertThatIllegalStateException()
                .isThrownBy(() -> RetentionPolicyCheck.verify(longReplay, TOPICS))
                .withMessageContaining("the replay window");
    }
}
