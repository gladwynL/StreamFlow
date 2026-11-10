package io.github.gladwynl.streamflow;

import io.github.gladwynl.streamflow.config.Role;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point for every StreamFlow role. The role is selected by activating exactly one of the Spring profiles
 * {@code migrate}, {@code api} or {@code worker}.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class StreamflowApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StreamflowApplication.class, args);
        if (Role.of(context.getEnvironment()) == Role.MIGRATE) {
            // migrate is a one-shot job: exit once migrations and topic provisioning have completed.
            System.exit(SpringApplication.exit(context));
        }
    }
}
