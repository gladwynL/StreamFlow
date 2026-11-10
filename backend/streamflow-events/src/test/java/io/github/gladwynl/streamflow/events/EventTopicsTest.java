package io.github.gladwynl.streamflow.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

class EventTopicsTest {

    @Test
    void deadLetterTopicAppendsSuffix() {
        assertThat(EventTopics.EVENTS_DEAD_LETTER).isEqualTo("streamflow.events.v1.dlt");
        assertThat(EventTopics.deadLetterTopicFor("orders")).isEqualTo("orders.dlt");
    }

    @Test
    void rejectsBlankOrNullSourceTopic() {
        assertThatIllegalArgumentException().isThrownBy(() -> EventTopics.deadLetterTopicFor(" "));
        assertThatNullPointerException().isThrownBy(() -> EventTopics.deadLetterTopicFor(null));
    }

    @Test
    void rejectsDeadLetterOfDeadLetter() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> EventTopics.deadLetterTopicFor(EventTopics.EVENTS_DEAD_LETTER))
                .withMessageContaining("streamflow.events.v1.dlt");
    }
}
