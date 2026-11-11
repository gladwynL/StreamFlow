package io.github.gladwynl.streamflow.config;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.springframework.core.env.Environment;

/**
 * The runtime roles of the StreamFlow application. Each process runs exactly one role, selected by activating the
 * Spring profile of the same name.
 */
public enum Role {
    /** One-shot job: applies database migrations and provisions Kafka topics, then exits. */
    MIGRATE,
    /** REST gateway, system status and partition maintenance. */
    API,
    /** Consumes, processes and persists events; reports liveness through heartbeats. */
    WORKER,
    /** One-shot job: publishes one bounded, seeded simulation run, reconciles it, then exits. */
    GENERATE;

    public String profile() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** One-shot roles exit when their work is done; their exit status reports the outcome. */
    public boolean oneShot() {
        return this == MIGRATE || this == GENERATE;
    }

    /**
     * Resolves the single active role.
     *
     * @throws IllegalStateException if no role profile, or more than one, is active
     */
    public static Role of(Environment environment) {
        List<String> activeProfiles = Arrays.asList(environment.getActiveProfiles());
        List<Role> active = Arrays.stream(values())
                .filter(role -> activeProfiles.contains(role.profile()))
                .toList();
        if (active.size() == 1) {
            return active.getFirst();
        }
        String allowed = Arrays.stream(values()).map(Role::profile).collect(Collectors.joining(", "));
        if (active.isEmpty()) {
            throw new IllegalStateException(
                    "No StreamFlow role is active. Set SPRING_PROFILES_ACTIVE to exactly one of: " + allowed);
        }
        String found = active.stream().map(Role::profile).collect(Collectors.joining(", "));
        throw new IllegalStateException(
                "Exactly one StreamFlow role may be active, but found: " + found + ". Allowed roles: " + allowed);
    }
}
