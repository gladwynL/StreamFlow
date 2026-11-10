package io.github.gladwynl.streamflow.kafka;

/**
 * A managed topic config whose actual value differs from the desired one.
 *
 * @param actual the current value, or {@code null} if the broker reported none
 */
public record ConfigDrift(String key, String expected, String actual) {}
