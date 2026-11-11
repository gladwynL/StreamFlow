package io.github.gladwynl.streamflow.simulation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.persistence.RunRepository.FailureOutcome;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

class PublishFailureClassifierTest {

    @Test
    void anythingThrownBySendIsDefinite() {
        assertThat(PublishFailureClassifier.classify(new IllegalStateException("closed"), true))
                .isEqualTo(FailureOutcome.DEFINITE);
    }

    @Test
    void clientSideRejectionsAreDefinite() {
        assertThat(PublishFailureClassifier.classify(new SerializationException("x"), false))
                .isEqualTo(FailureOutcome.DEFINITE);
        assertThat(PublishFailureClassifier.classify(new RecordTooLargeException("x"), false))
                .isEqualTo(FailureOutcome.DEFINITE);
        assertThat(PublishFailureClassifier.classify(new BufferExhaustedException("x"), false))
                .isEqualTo(FailureOutcome.DEFINITE);
    }

    @Test
    void timeoutsAndBrokerErrorsAreAmbiguous() {
        assertThat(PublishFailureClassifier.classify(new TimeoutException("delivery timeout"), false))
                .isEqualTo(FailureOutcome.AMBIGUOUS);
        assertThat(PublishFailureClassifier.classify(new NetworkException("reset"), false))
                .isEqualTo(FailureOutcome.AMBIGUOUS);
        assertThat(PublishFailureClassifier.classify(new NotLeaderOrFollowerException("moved"), false))
                .isEqualTo(FailureOutcome.AMBIGUOUS);
    }
}
