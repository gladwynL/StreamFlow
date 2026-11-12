package io.github.gladwynl.streamflow.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class RoleTest {

    @Test
    void resolvesTheSingleActiveRole() {
        assertThat(Role.of(environment("worker"))).isEqualTo(Role.WORKER);
        assertThat(Role.of(environment("api", "local"))).isEqualTo(Role.API);
        assertThat(Role.of(environment("migrate"))).isEqualTo(Role.MIGRATE);
    }

    @Test
    void rejectsMissingRole() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Role.of(environment("local")))
                .withMessageContaining("No StreamFlow role is active")
                .withMessageContaining("migrate, api, worker, generate");
    }

    @Test
    void rejectsMoreThanOneRole() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Role.of(environment("api", "worker")))
                .withMessageContaining("Exactly one StreamFlow role")
                .withMessageContaining("api, worker");
    }

    @Test
    void profileNameIsLowerCase() {
        assertThat(Role.WORKER.profile()).isEqualTo("worker");
        assertThat(Role.GENERATE.profile()).isEqualTo("generate");
    }

    @Test
    void onlyMigrateAndGenerateAreOneShot() {
        assertThat(Role.values()).filteredOn(Role::oneShot).containsExactly(Role.MIGRATE, Role.GENERATE, Role.LOADGEN);
    }

    private static MockEnvironment environment(String... profiles) {
        MockEnvironment environment = new MockEnvironment();
        environment.setActiveProfiles(profiles);
        return environment;
    }
}
