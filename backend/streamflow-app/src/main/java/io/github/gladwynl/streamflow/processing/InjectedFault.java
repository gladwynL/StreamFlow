package io.github.gladwynl.streamflow.processing;

/** A failure raised on purpose by a fault-injection scenario. */
public class InjectedFault extends RuntimeException {

    private final FailureCategory category;

    public InjectedFault(FailureCategory category, String message) {
        super(message);
        this.category = category;
    }

    public FailureCategory category() {
        return category;
    }
}
