package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.events.EventDecodingException;
import io.github.gladwynl.streamflow.events.EventValidationException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Set;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * Maps a processing failure to its {@link FailureCategory} by walking the cause chain.
 *
 * <ul>
 *   <li>Bad record: decoding and validation failures; PostgreSQL data exceptions (SQLSTATE class {@code 22}) and
 *       integrity violations (class {@code 23}); injected rejections.
 *   <li>Transient: serialization failure {@code 40001}, deadlock {@code 40P01}, lock not available {@code 55P03},
 *       statement timeout {@code 57014}; injected transient faults; and, conservatively, any failure not recognized
 *       below — retried a bounded number of times, then dead-lettered rather than retried forever.
 *   <li>Infrastructure: connection errors (class {@code 08}), server shutdown or start-up ({@code 57P01}–{@code
 *       57P03}), too many connections ({@code 53300}), insufficient resources (class {@code 53}), pool or
 *       transaction-start failures, and socket timeouts; and Kafka-side failures — a failed offset commit
 *       ({@code CommitFailedException}, {@code RebalanceInProgressException}) or any retriable Kafka error. These are
 *       never a record's fault: the record is redelivered and, if it had committed, suppressed by the ledger.
 * </ul>
 */
public final class FailureClassifier {

    private static final Set<String> TRANSIENT_STATES = Set.of("40001", "40P01", "55P03", "57014");
    private static final Set<String> INFRASTRUCTURE_STATES = Set.of("57P01", "57P02", "57P03");

    private FailureClassifier() {}

    public static FailureCategory classify(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof InjectedFault injected) {
                return injected.category();
            }
            if (t instanceof EventDecodingException || t instanceof EventValidationException) {
                return FailureCategory.BAD_RECORD;
            }
            if (t instanceof CommitFailedException
                    || t instanceof RebalanceInProgressException
                    || t instanceof RetriableException) {
                return FailureCategory.INFRASTRUCTURE;
            }
            if (t instanceof CannotGetJdbcConnectionException
                    || t instanceof CannotCreateTransactionException
                    || t instanceof ConnectException
                    || t instanceof SocketTimeoutException) {
                return FailureCategory.INFRASTRUCTURE;
            }
            if (t instanceof SQLException sql && sql.getSQLState() != null) {
                FailureCategory category = classifySqlState(sql.getSQLState());
                if (category != null) {
                    return category;
                }
            }
        }
        return FailureCategory.TRANSIENT;
    }

    static FailureCategory classifySqlState(String state) {
        if (state.startsWith("08") || state.startsWith("53") || INFRASTRUCTURE_STATES.contains(state)) {
            return FailureCategory.INFRASTRUCTURE;
        }
        if (TRANSIENT_STATES.contains(state)) {
            return FailureCategory.TRANSIENT;
        }
        if (state.startsWith("22") || state.startsWith("23")) {
            return FailureCategory.BAD_RECORD;
        }
        return null;
    }
}
