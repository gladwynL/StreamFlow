package io.github.gladwynl.streamflow;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Keeps the runtime roles separable: role packages never depend on each other, and shared packages never depend on
 * a role.
 */
@AnalyzeClasses(packages = "io.github.gladwynl.streamflow", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    private static final String API = "io.github.gladwynl.streamflow.api..";
    private static final String MIGRATE = "io.github.gladwynl.streamflow.migrate..";
    private static final String WORKER = "io.github.gladwynl.streamflow.worker..";
    private static final String GENERATE = "io.github.gladwynl.streamflow.generate..";
    private static final String LOADGEN = "io.github.gladwynl.streamflow.loadgen..";

    @ArchTest
    static final ArchRule apiDoesNotDependOnOtherRoles = roleIsolated(API, MIGRATE, WORKER, GENERATE, LOADGEN);

    @ArchTest
    static final ArchRule migrateDoesNotDependOnOtherRoles = roleIsolated(MIGRATE, API, WORKER, GENERATE, LOADGEN);

    @ArchTest
    static final ArchRule workerDoesNotDependOnOtherRoles = roleIsolated(WORKER, API, MIGRATE, GENERATE, LOADGEN);

    @ArchTest
    static final ArchRule generateDoesNotDependOnOtherRoles = roleIsolated(GENERATE, API, MIGRATE, WORKER, LOADGEN);

    @ArchTest
    static final ArchRule loadgenDoesNotDependOnOtherRoles = roleIsolated(LOADGEN, API, MIGRATE, WORKER, GENERATE);

    @ArchTest
    static final ArchRule sharedPackagesDoNotDependOnRoles = noClasses()
            .that()
            .resideInAnyPackage(
                    "io.github.gladwynl.streamflow.config..",
                    "io.github.gladwynl.streamflow.kafka..",
                    "io.github.gladwynl.streamflow.persistence..",
                    "io.github.gladwynl.streamflow.processing..",
                    "io.github.gladwynl.streamflow.simulation..",
                    "io.github.gladwynl.streamflow.reconciliation..",
                    "io.github.gladwynl.streamflow.deadletter..",
                    "io.github.gladwynl.streamflow.events..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(API, MIGRATE, WORKER, GENERATE, LOADGEN);

    /** Only processing code writes events; the API and the generator never touch the event store. */
    @ArchTest
    static final ArchRule onlyProcessingWritesEvents = noClasses()
            .that()
            .resideOutsideOfPackages(
                    "io.github.gladwynl.streamflow.processing..", "io.github.gladwynl.streamflow.persistence..")
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("io.github.gladwynl.streamflow.persistence.EventStore");

    @ArchTest
    static final ArchRule eventContractsDoNotDependOnSpring = noClasses()
            .that()
            .resideInAPackage("io.github.gladwynl.streamflow.events..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.springframework..");

    private static ArchRule roleIsolated(String role, String... otherRoles) {
        return noClasses()
                .that()
                .resideInAPackage(role)
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage(otherRoles);
    }
}
