package io.github.gladwynl.streamflow.simulation;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live producer-side counters of one run, readable while the run is in progress, and its cooperative stop flag.
 * Thread-safe: the publisher thread and the producer's callback thread update it; any thread may read it or request
 * a stop.
 */
public final class RunProgress {

    final AtomicLong attempted = new AtomicLong();
    final AtomicLong acked = new AtomicLong();
    final AtomicLong definite = new AtomicLong();
    final AtomicLong ambiguous = new AtomicLong();
    private volatile UUID runId;
    private volatile boolean stopRequested;
    private volatile boolean finished;

    void started(UUID runId) {
        this.runId = runId;
    }

    void finished() {
        this.finished = true;
    }

    /** Asks the publisher to stop after the current event; the run then ends as INCOMPLETE. */
    public void requestStop() {
        stopRequested = true;
    }

    public boolean stopRequested() {
        return stopRequested;
    }

    /** Null until the run row exists. */
    public UUID runId() {
        return runId;
    }

    public boolean isFinished() {
        return finished;
    }

    public long attempted() {
        return attempted.get();
    }

    public long acked() {
        return acked.get();
    }

    public long failedDefinite() {
        return definite.get();
    }

    public long failedAmbiguous() {
        return ambiguous.get();
    }

    long failures() {
        return definite.get() + ambiguous.get();
    }

    long outcomes() {
        return acked.get() + failures();
    }
}
