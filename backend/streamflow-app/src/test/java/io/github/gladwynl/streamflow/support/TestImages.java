package io.github.gladwynl.streamflow.support;

/**
 * Container images used by integration tests. They must match the images in {@code .env.example} used by Docker
 * Compose; {@link ContainerImageVersionsTest} enforces that.
 */
public final class TestImages {

    public static final String KAFKA = "apache/kafka:4.3.2";
    public static final String POSTGRES = "postgres:18.6-alpine";

    private TestImages() {}
}
