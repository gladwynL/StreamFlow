package io.github.gladwynl.streamflow.processing;

public enum ProcessingResult {
    /** First delivery: the ledger row and every effect were committed. */
    PROCESSED,
    /** Redelivery of an event already in the ledger: no effect was applied. */
    DUPLICATE
}
