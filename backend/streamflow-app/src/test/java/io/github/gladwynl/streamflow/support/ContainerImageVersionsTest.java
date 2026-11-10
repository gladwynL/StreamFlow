package io.github.gladwynl.streamflow.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Integration tests and Docker Compose must run the same Kafka and PostgreSQL versions. */
class ContainerImageVersionsTest {

    // Surefire runs with the module directory as working directory.
    private static final Path ENV_EXAMPLE = Path.of("..", "..", ".env.example");

    @Test
    void testImagesMatchComposeImages() throws IOException {
        Map<String, String> env = Files.readAllLines(ENV_EXAMPLE).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#") && line.contains("="))
                .collect(Collectors.toMap(
                        line -> line.substring(0, line.indexOf('=')), line -> line.substring(line.indexOf('=') + 1)));

        assertThat(env)
                .containsEntry("KAFKA_IMAGE", TestImages.KAFKA)
                .containsEntry("POSTGRES_IMAGE", TestImages.POSTGRES);
    }
}
