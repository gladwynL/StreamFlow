package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.persistence.RunRepository.FailureOutcome;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Classifies a failed publication as {@code DEFINITE} (the record never reached the broker) or {@code AMBIGUOUS}
 * (the broker may have stored it).
 *
 * <p>The classification is deliberately conservative. Reconciliation treats a {@code DEFINITE} record that later
 * appears in the ledger as an error, while an {@code AMBIGUOUS} record may legitimately appear or not. Only failures
 * that provably happen before the record is handed to the network are therefore {@code DEFINITE}; every other error,
 * including all timeouts, is {@code AMBIGUOUS}.
 */
public final class PublishFailureClassifier {

    private PublishFailureClassifier() {}

    /**
     * @param thrownBySend {@code true} if {@code Producer.send} threw instead of reporting through its callback
     */
    public static FailureOutcome classify(Throwable error, boolean thrownBySend) {
        if (thrownBySend) {
            return FailureOutcome.DEFINITE;
        }
        if (error instanceof SerializationException
                || error instanceof RecordTooLargeException
                || error instanceof BufferExhaustedException
                || error instanceof InterruptException) {
            return FailureOutcome.DEFINITE;
        }
        return FailureOutcome.AMBIGUOUS;
    }
}
