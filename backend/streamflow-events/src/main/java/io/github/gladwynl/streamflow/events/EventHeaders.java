package io.github.gladwynl.streamflow.events;

/**
 * Kafka record headers carried by every StreamFlow event. They duplicate envelope fields on purpose: a record whose
 * value cannot be decoded can still be attributed to its run and sequence number.
 */
public final class EventHeaders {

    public static final String RUN_ID = "sf-run-id";
    public static final String EVENT_ID = "sf-event-id";
    public static final String SEQUENCE = "sf-seq";
    public static final String SCHEMA_VERSION = "sf-schema-version";

    private EventHeaders() {}
}
