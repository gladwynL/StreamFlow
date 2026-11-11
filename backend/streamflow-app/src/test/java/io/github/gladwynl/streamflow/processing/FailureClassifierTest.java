package io.github.gladwynl.streamflow.processing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gladwynl.streamflow.events.EventDecodingException;
import io.github.gladwynl.streamflow.events.EventValidationException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.CannotCreateTransactionException;

class FailureClassifierTest {

    @Test
    void decodingAndValidationFailuresAreBadRecords() {
        assertThat(classify(new EventDecodingException("bad json"))).isEqualTo(FailureCategory.BAD_RECORD);
        assertThat(classify(new EventValidationException(List.of("x")))).isEqualTo(FailureCategory.BAD_RECORD);
    }

    @Test
    void dataAndIntegrityErrorsAreBadRecords() {
        assertThat(classify(sql("22001"))).isEqualTo(FailureCategory.BAD_RECORD);
        assertThat(classify(new DataIntegrityViolationException("x", new SQLException("x", "23514"))))
                .isEqualTo(FailureCategory.BAD_RECORD);
    }

    @Test
    void conflictsAndTimeoutsOfOneStatementAreTransient() {
        assertThat(classify(sql("40001"))).isEqualTo(FailureCategory.TRANSIENT);
        assertThat(classify(sql("40P01"))).isEqualTo(FailureCategory.TRANSIENT);
        assertThat(classify(sql("55P03"))).isEqualTo(FailureCategory.TRANSIENT);
        assertThat(classify(new QueryTimeoutException("x", new SQLException("x", "57014"))))
                .isEqualTo(FailureCategory.TRANSIENT);
    }

    @Test
    void unavailableDependenciesAreInfrastructure() {
        assertThat(classify(sql("08006"))).isEqualTo(FailureCategory.INFRASTRUCTURE);
        assertThat(classify(sql("57P01"))).isEqualTo(FailureCategory.INFRASTRUCTURE);
        assertThat(classify(sql("53300"))).isEqualTo(FailureCategory.INFRASTRUCTURE);
        assertThat(classify(new CannotGetJdbcConnectionException("pool timeout")))
                .isEqualTo(FailureCategory.INFRASTRUCTURE);
        assertThat(classify(new CannotCreateTransactionException("x"))).isEqualTo(FailureCategory.INFRASTRUCTURE);
        assertThat(classify(new RuntimeException(new SocketTimeoutException("read timed out"))))
                .isEqualTo(FailureCategory.INFRASTRUCTURE);
    }

    @Test
    void injectedFaultsCarryTheirCategory() {
        assertThat(classify(new InjectedFault(FailureCategory.BAD_RECORD, "x"))).isEqualTo(FailureCategory.BAD_RECORD);
        assertThat(classify(new InjectedFault(FailureCategory.TRANSIENT, "x"))).isEqualTo(FailureCategory.TRANSIENT);
    }

    @Test
    void unrecognizedFailuresAreTransientSoTheyAreRetriedThenDeadLetteredNotRetriedForever() {
        assertThat(classify(new IllegalStateException("bug"))).isEqualTo(FailureCategory.TRANSIENT);
        assertThat(classify(sql("XX000"))).isEqualTo(FailureCategory.TRANSIENT);
    }

    /** The listener container wraps listener exceptions; classification looks through the wrapper. */
    private static FailureCategory classify(Throwable cause) {
        return FailureClassifier.classify(new ListenerExecutionFailedException("listener failed", cause));
    }

    private static RuntimeException sql(String state) {
        return new RuntimeException(new SQLException("failure", state));
    }
}
