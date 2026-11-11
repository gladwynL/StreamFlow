package io.github.gladwynl.streamflow.config;

import io.github.gladwynl.streamflow.config.StreamflowProperties.Retention;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The deduplication guarantee holds only while ledger rows outlive every way an event can come back: redelivery
 * from a topic (bounded by its retention) and replay of a dead letter (bounded by the replay window). Every role
 * checks this at startup and refuses to start if the configuration breaks it (ADR-0008).
 */
public final class RetentionPolicyCheck {

    private RetentionPolicyCheck() {}

    /**
     * @param topicRetention retention of each topic an event can be redelivered from, by topic name
     * @throws IllegalStateException if the ledger retention is shorter than a window plus the margin
     */
    public static void verify(Retention retention, Map<String, Duration> topicRetention) {
        List<String> problems = new ArrayList<>();
        Duration required = retention.replayWindow();
        String requiredBy = "the replay window";
        for (Map.Entry<String, Duration> topic : topicRetention.entrySet()) {
            if (topic.getValue().compareTo(required) > 0) {
                required = topic.getValue();
                requiredBy = "retention of topic " + topic.getKey();
            }
        }
        Duration minimum = required.plus(retention.margin());
        if (retention.ledger().compareTo(minimum) < 0) {
            problems.add("ledger retention %s is shorter than %s (%s) plus margin %s"
                    .formatted(retention.ledger(), requiredBy, required, retention.margin()));
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException(
                    "Retention settings would break the deduplication guarantee: " + String.join("; ", problems));
        }
    }
}
