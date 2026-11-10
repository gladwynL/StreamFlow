package io.github.gladwynl.streamflow.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicSpecTest {

    private static final TopicSpec SPEC =
            new TopicSpec("events", 6, (short) 1, Map.of("retention.ms", "86400000", "cleanup.policy", "delete"));

    @Test
    void reportsNoDriftWhenManagedConfigsMatch() {
        TopicState state = new TopicState(
                "events", 6, 1, Map.of("retention.ms", "86400000", "cleanup.policy", "delete", "other", "x"));

        assertThat(SPEC.configDrift(state)).isEmpty();
    }

    @Test
    void reportsChangedAndMissingConfigsOrderedByKey() {
        TopicState state = new TopicState("events", 6, 1, Map.of("retention.ms", "1000"));

        assertThat(SPEC.configDrift(state))
                .containsExactly(
                        new ConfigDrift("cleanup.policy", "delete", null),
                        new ConfigDrift("retention.ms", "86400000", "1000"));
    }

    @Test
    void rejectsInvalidSpecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> new TopicSpec(" ", 1, (short) 1, Map.of()));
        assertThatIllegalArgumentException().isThrownBy(() -> new TopicSpec("t", 0, (short) 1, Map.of()));
        assertThatIllegalArgumentException().isThrownBy(() -> new TopicSpec("t", 1, (short) 0, Map.of()));
    }

    @Test
    void treatsMissingConfigsAsEmpty() {
        assertThat(new TopicSpec("t", 1, (short) 1, null).configs()).isEmpty();
    }
}
