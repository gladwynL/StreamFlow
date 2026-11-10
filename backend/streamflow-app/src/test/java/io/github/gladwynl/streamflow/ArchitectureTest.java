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

    @ArchTest
    static final ArchRule apiDoesNotDependOnOtherRoles = roleIsolated(API, MIGRATE, WORKER);

    @ArchTest
    static final ArchRule migrateDoesNotDependOnOtherRoles = roleIsolated(MIGRATE, API, WORKER);

    @ArchTest
    static final ArchRule workerDoesNotDependOnOtherRoles = roleIsolated(WORKER, API, MIGRATE);

    @ArchTest
    static final ArchRule sharedPackagesDoNotDependOnRoles = noClasses()
            .that()
            .resideInAnyPackage(
                    "io.github.gladwynl.streamflow.config..",
                    "io.github.gladwynl.streamflow.kafka..",
                    "io.github.gladwynl.streamflow.persistence..",
                    "io.github.gladwynl.streamflow.events..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(API, MIGRATE, WORKER);

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
