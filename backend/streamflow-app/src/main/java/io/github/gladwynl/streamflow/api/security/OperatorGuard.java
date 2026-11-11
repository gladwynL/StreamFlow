package io.github.gladwynl.streamflow.api.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import java.security.MessageDigest;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * Guards operator actions (replay, discard) under the local security model: the api is reachable only on
 * 127.0.0.1, operator actions require the configured key in {@value #HEADER}, and they are rate-limited per api
 * process. Without a configured key, operator actions are disabled.
 */
@Component
@Profile("api")
public class OperatorGuard {

    public static final String HEADER = "X-StreamFlow-Operator-Key";

    private final byte[] operatorKey;
    private final TokenBucket rateLimit;

    public OperatorGuard(StreamflowProperties properties) {
        String key = properties.api().operatorKey();
        this.operatorKey = key == null || key.isBlank() ? null : key.getBytes(UTF_8);
        this.rateLimit = new TokenBucket(properties.api().operatorRequestsPerMinute());
    }

    /**
     * @throws ResponseStatusException 503 if operator actions are disabled, 401 for a missing or wrong key, 429 when
     *     the rate limit is exceeded
     */
    public void authorize(String presentedKey) {
        if (operatorKey == null) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Operator actions are disabled: set STREAMFLOW_API_OPERATOR_KEY to enable them");
        }
        if (presentedKey == null || !MessageDigest.isEqual(operatorKey, presentedKey.getBytes(UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing or invalid " + HEADER);
        }
        if (!rateLimit.tryAcquire()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Operator rate limit exceeded");
        }
    }

    /** A token bucket refilled continuously at {@code perMinute} tokens per minute, holding at most that many. */
    static final class TokenBucket {

        private final double capacity;
        private final double refillPerNano;
        private double tokens;
        private long lastRefill = System.nanoTime();

        TokenBucket(int perMinute) {
            if (perMinute < 1) {
                throw new IllegalArgumentException("operator rate limit must be at least 1 per minute");
            }
            this.capacity = perMinute;
            this.refillPerNano = perMinute / 60_000_000_000.0;
            this.tokens = perMinute;
        }

        synchronized boolean tryAcquire() {
            long now = System.nanoTime();
            tokens = Math.min(capacity, tokens + (now - lastRefill) * refillPerNano);
            lastRefill = now;
            if (tokens < 1) {
                return false;
            }
            tokens -= 1;
            return true;
        }
    }
}
