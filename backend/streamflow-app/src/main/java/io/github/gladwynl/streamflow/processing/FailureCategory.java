package io.github.gladwynl.streamflow.processing;

/** How a processing failure is handled (ADR-0006). */
public enum FailureCategory {
    /** The record itself is invalid; retrying cannot help. Dead-lettered on the first attempt. */
    BAD_RECORD,
    /** Specific to this record and likely to pass on retry. Bounded retries in place, then dead-lettered. */
    TRANSIENT,
    /** A dependency is unavailable. Retried without limit; never dead-lettered. The partition waits. */
    INFRASTRUCTURE
}
