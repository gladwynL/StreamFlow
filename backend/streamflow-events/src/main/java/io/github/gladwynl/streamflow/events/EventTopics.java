package io.github.gladwynl.streamflow.events;

import java.util.Objects;

/**
 * Kafka topic names that form part of the StreamFlow event contract.
 *
 * <p>Producers and consumers in every role refer to these names. The operational settings of each topic (partition
 * count, retention) belong to the application configuration, not to the contract.
 */
public final class EventTopics {

    /** Main event stream. Version suffix changes only with an incompatible envelope change. */
    public static final String EVENTS = "streamflow.events.v1";

    /** Suffix appended to a source topic to name its dead-letter topic. */
    public static final String DEAD_LETTER_SUFFIX = ".dlt";

    /** Dead-letter topic for {@link #EVENTS}. */
    public static final String EVENTS_DEAD_LETTER = deadLetterTopicFor(EVENTS);

    private EventTopics() {}

    /**
     * Returns the dead-letter topic name for a source topic.
     *
     * @throws IllegalArgumentException if the source topic is blank or is itself a dead-letter topic
     */
    public static String deadLetterTopicFor(String sourceTopic) {
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        if (sourceTopic.isBlank()) {
            throw new IllegalArgumentException("sourceTopic must not be blank");
        }
        if (sourceTopic.endsWith(DEAD_LETTER_SUFFIX)) {
            throw new IllegalArgumentException(
                    "Dead-letter topics do not have their own dead-letter topic: " + sourceTopic);
        }
        return sourceTopic + DEAD_LETTER_SUFFIX;
    }
}
