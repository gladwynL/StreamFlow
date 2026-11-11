package io.github.gladwynl.streamflow.api.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class OperatorGuardTest {

    @Test
    void operatorActionsAreDisabledWithoutAConfiguredKey() {
        OperatorGuard guard = new OperatorGuard(properties(null, 30));

        assertStatus(() -> guard.authorize("anything"), HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void rejectsAMissingOrWrongKey() {
        OperatorGuard guard = new OperatorGuard(properties("s3cret-key", 30));

        assertStatus(() -> guard.authorize(null), HttpStatus.UNAUTHORIZED);
        assertStatus(() -> guard.authorize("s3cret-kez"), HttpStatus.UNAUTHORIZED);
        assertThatCode(() -> guard.authorize("s3cret-key")).doesNotThrowAnyException();
    }

    @Test
    void ratesLimitsAuthorizedRequests() {
        OperatorGuard guard = new OperatorGuard(properties("k", 2));

        guard.authorize("k");
        guard.authorize("k");
        assertStatus(() -> guard.authorize("k"), HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void tokenBucketRefillsOverTime() throws InterruptedException {
        OperatorGuard.TokenBucket bucket = new OperatorGuard.TokenBucket(600); // 10 per second

        for (int i = 0; i < 600; i++) {
            assertThat(bucket.tryAcquire()).isTrue();
        }
        assertThat(bucket.tryAcquire()).isFalse();
        Thread.sleep(250);
        assertThat(bucket.tryAcquire()).isTrue();
    }

    private static StreamflowProperties properties(String key, int perMinute) {
        java.util.Map<String, String> source = new java.util.HashMap<>();
        if (key != null) {
            source.put("streamflow.api.operator-key", key);
        }
        source.put("streamflow.api.operator-requests-per-minute", Integer.toString(perMinute));
        source.put("streamflow.kafka.topics.events.partitions", "6");
        return new Binder(new MapConfigurationPropertySource(source))
                .bindOrCreate("streamflow", StreamflowProperties.class);
    }

    private static void assertStatus(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, HttpStatus status) {
        assertThatThrownBy(call)
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(e -> assertThat(((ResponseStatusException) e).getStatusCode())
                        .isEqualTo(status));
    }
}
