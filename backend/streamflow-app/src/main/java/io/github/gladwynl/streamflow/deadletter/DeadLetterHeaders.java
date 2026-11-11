package io.github.gladwynl.streamflow.deadletter;

/** Headers StreamFlow adds to dead-letter and replayed records, next to Spring's {@code kafka_dlt-*} headers. */
public final class DeadLetterHeaders {

    /** {@code BAD_RECORD} or {@code TRANSIENT}: why the record was dead-lettered. */
    public static final String FAILURE_CATEGORY = "sf-failure-category";
    /** Delivery attempts made by the consumer before dead-lettering (per partition assignment). */
    public static final String DELIVERY_ATTEMPTS = "sf-delivery-attempts";
    /** On a replayed record: the id of the dead letter it replays. */
    public static final String REPLAY_OF = "sf-replay-of";

    private DeadLetterHeaders() {}
}
