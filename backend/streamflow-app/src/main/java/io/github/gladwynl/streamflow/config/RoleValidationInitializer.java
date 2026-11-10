package io.github.gladwynl.streamflow.config;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Fails startup before any bean is created unless exactly one role profile is active. Registered in
 * {@code META-INF/spring.factories}, so it also applies to Spring Boot tests.
 */
public class RoleValidationInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        Role.of(context.getEnvironment());
    }
}
