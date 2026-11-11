package io.github.gladwynl.streamflow.generate;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.persistence.RunRepository.RunStatus;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationReport;
import io.github.gladwynl.streamflow.reconciliation.ReconciliationService;
import io.github.gladwynl.streamflow.simulation.RunSummary;
import io.github.gladwynl.streamflow.simulation.SimulationService;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Publishes one simulation run, waits for the workers to drain it, and reconciles it. The process exit status is
 * the verdict: 0 PASS, 2 FAIL, 3 INCOMPLETE.
 */
@Component
@Profile("generate")
class GenerateRunner implements ApplicationRunner, ExitCodeGenerator {

    static final int EXIT_PASS = 0;
    static final int EXIT_FAIL = 2;
    static final int EXIT_INCOMPLETE = 3;

    private static final Logger log = LoggerFactory.getLogger(GenerateRunner.class);

    private final SimulationService simulation;
    private final ReconciliationService reconciliation;
    private final StreamflowProperties.Generate settings;
    private volatile int exitCode = EXIT_INCOMPLETE;
    private volatile RunSummary summary;
    private volatile ReconciliationReport report;

    GenerateRunner(
            SimulationService simulation, ReconciliationService reconciliation, StreamflowProperties properties) {
        this.simulation = simulation;
        this.reconciliation = reconciliation;
        this.settings = properties.generate();
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        SimulationSpec spec =
                new SimulationSpec(settings.seed(), settings.events(), settings.ratePerSecond(), settings.users());
        summary = simulation.run(spec);
        log.info(
                "Published run {}: attempted={}, acked={}, failedDefinite={}, failedAmbiguous={}, contentHash={},"
                        + " elapsed={}",
                summary.runId(),
                summary.attempted(),
                summary.acked(),
                summary.failedDefinite(),
                summary.failedAmbiguous(),
                summary.contentHash(),
                summary.elapsed());
        if (summary.status() != RunStatus.COMPLETED) {
            log.warn("Run {} did not complete: {}", summary.runId(), summary.statusDetail());
        }

        report = reconciliation.awaitDrainAndReconcile(summary.runId(), settings.drainTimeout());
        exitCode = switch (report.result()) {
            case PASS -> EXIT_PASS;
            case FAIL -> EXIT_FAIL;
            case INCOMPLETE -> EXIT_INCOMPLETE;
        };
        log.info("Reconciliation of run {}: {} {}", summary.runId(), report.result(), report.reasons());
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    RunSummary summary() {
        return summary;
    }

    ReconciliationReport report() {
        return report;
    }
}
