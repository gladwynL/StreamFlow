package io.github.gladwynl.streamflow.events;

import java.util.List;

/** A decoded event breaks one or more contract rules. */
public class EventValidationException extends RuntimeException {

    private final List<String> violations;

    public EventValidationException(List<String> violations) {
        super("Invalid event: " + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
