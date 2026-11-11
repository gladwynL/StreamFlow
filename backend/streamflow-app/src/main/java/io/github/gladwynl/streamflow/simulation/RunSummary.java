package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import java.time.Duration;
import java.util.UUID;

/**
 * Producer-side result of a run. {@code attempted == acked + failedDefinite + failedAmbiguous} for a completed run.
 *
 * @param contentHash SHA-256 of the generated content; equal for equal seed, spec and content version
 */
public record RunSummary(
        UUID runId,
        RunStatus status,
        String statusDetail,
        long attempted,
        long acked,
        long failedDefinite,
        long failedAmbiguous,
        String contentHash,
        Duration elapsed) {}
