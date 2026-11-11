package io.github.gladwynl.streamflow;

import io.github.gladwynl.streamflow.config.Role;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point for every StreamFlow role. The role is selected by activating exactly one of the Spring profiles
 * {@code migrate}, {@code api}, {@code worker} or {@code generate}.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class StreamflowApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StreamflowApplication.class, args);
        if (Role.of(context.getEnvironment()).oneShot()) {
            // One-shot roles have finished their work in application runners; exit with their status.
            System.exit(SpringApplication.exit(context));
        }
    }
}
