package io.github.gladwynl.streamflow.config;

import org.flywaydb.core.Flyway;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Only the migrate role changes the schema. The long-running roles validate it instead, so they refuse to start
 * against a database with pending or modified migrations.
 */
@Configuration(proxyBeanMethods = false)
@Profile({"api", "worker", "generate"})
class FlywayValidationConfiguration {

    @Bean
    FlywayMigrationStrategy validateOnlyMigrationStrategy() {
        return Flyway::validate;
    }
}
