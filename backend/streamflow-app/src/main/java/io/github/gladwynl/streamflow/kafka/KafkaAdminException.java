package io.github.gladwynl.streamflow.kafka;

/** A Kafka admin request failed or did not complete in time. Usually transient while the broker starts. */
public class KafkaAdminException extends RuntimeException {

    public KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
