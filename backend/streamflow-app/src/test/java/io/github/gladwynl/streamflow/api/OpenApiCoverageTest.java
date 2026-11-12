package io.github.gladwynl.streamflow.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.yaml.snakeyaml.Yaml;

/**
 * docs/openapi.yaml describes exactly the operations the controllers serve: a new or removed endpoint without an
 * update of the description fails the build (Phase 4 acceptance criterion "OpenAPI published", Phase 6).
 */
class OpenApiCoverageTest {

    private static final Path SPEC = Path.of("..", "..", "docs", "openapi.yaml");

    @Test
    void theOpenApiDescriptionMatchesTheControllers() throws Exception {
        assertThat(documented()).isEqualTo(served());
    }

    @SuppressWarnings("unchecked")
    private static Set<String> documented() throws Exception {
        Set<String> operations = new TreeSet<>();
        try (InputStream in = Files.newInputStream(SPEC)) {
            Map<String, Object> spec = new Yaml().load(in);
            Map<String, Map<String, Object>> paths = (Map<String, Map<String, Object>>) spec.get("paths");
            paths.forEach((path, methods) -> methods.keySet()
                    .forEach(
                            method -> operations.add(method.toUpperCase() + " " + path.replaceAll("\\{[^}]+}", "{}"))));
        }
        return operations;
    }

    private static Set<String> served() throws Exception {
        // The controllers are @Profile("api"): scan as the api role does.
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles("api");
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false, environment);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));
        Set<String> operations = new TreeSet<>();
        for (var bean : scanner.findCandidateComponents("io.github.gladwynl.streamflow.api")) {
            Class<?> controller = Class.forName(bean.getBeanClassName());
            RequestMapping base = AnnotatedElementUtils.findMergedAnnotation(controller, RequestMapping.class);
            String prefix = base == null || base.path().length == 0 ? "" : base.path()[0];
            for (Method method : controller.getDeclaredMethods()) {
                RequestMapping mapping = AnnotatedElementUtils.findMergedAnnotation(method, RequestMapping.class);
                if (mapping == null) {
                    continue;
                }
                String path = prefix + (mapping.path().length == 0 ? "" : mapping.path()[0]);
                for (var verb : mapping.method()) {
                    operations.add(verb.name() + " " + path.replaceAll("\\{[^}]+}", "{}"));
                }
            }
        }
        return operations;
    }
}
