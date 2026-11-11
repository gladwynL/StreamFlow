package io.github.gladwynl.streamflow.api.live;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketExtension;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * Slow-client isolation of the live feed, with fake sessions: a client that never completes a send must not delay
 * the ticks or the other clients, must hold at most one snapshot, and must be disconnected.
 */
class LiveBroadcasterTest {

    @Test
    void aStalledClientIsSkippedThenClosedWhileOthersReceiveEveryTick() {
        AtomicInteger computed = new AtomicInteger();
        LiveBroadcaster broadcaster =
                new LiveBroadcaster(() -> "{\"n\":" + computed.incrementAndGet() + "}", 10, 60_000, 5);
        CountDownLatch never = new CountDownLatch(1);
        try {
            FakeSession fast = new FakeSession("fast", null);
            FakeSession stalled = new FakeSession("stalled", never);
            assertThat(broadcaster.register(fast)).isTrue();
            assertThat(broadcaster.register(stalled)).isTrue();

            for (int tick = 1; tick <= 8; tick++) {
                long start = System.nanoTime();
                broadcaster.tick();
                // The ticking thread never waits for a client.
                assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(500));
                int expected = tick;
                await().atMost(Duration.ofSeconds(5)).until(() -> fast.received.size() == expected);
            }

            assertThat(computed).hasValue(8); // computed once per tick, not once per client
            assertThat(stalled.sendAttempts).hasValue(1); // never more than one snapshot in flight
            await().atMost(Duration.ofSeconds(5)).until(() -> stalled.closeStatus.get() != null);
            assertThat(stalled.closeStatus.get().getCode()).isEqualTo(LiveBroadcaster.TOO_SLOW.getCode());
            assertThat(broadcaster.clientCount()).isEqualTo(1);
            LiveBroadcaster.Stats stats = broadcaster.stats();
            assertThat(stats.closedSlow()).isEqualTo(1);
            assertThat(stats.skipped()).isEqualTo(5);
        } finally {
            never.countDown();
            broadcaster.shutdown();
        }
    }

    @Test
    void connectionsBeyondTheLimitAreRefused() {
        LiveBroadcaster broadcaster = new LiveBroadcaster(() -> "{}", 2, 1_000, 5);
        try {
            assertThat(broadcaster.register(new FakeSession("a", null))).isTrue();
            assertThat(broadcaster.register(new FakeSession("b", null))).isTrue();
            FakeSession third = new FakeSession("c", null);
            assertThat(broadcaster.register(third)).isFalse();
            assertThat(third.closeStatus.get()).isEqualTo(LiveBroadcaster.TOO_MANY_CLIENTS);
            assertThat(broadcaster.stats().refused()).isEqualTo(1);
        } finally {
            broadcaster.shutdown();
        }
    }

    @Test
    void nothingIsComputedWithoutClients() {
        AtomicInteger computed = new AtomicInteger();
        LiveBroadcaster broadcaster = new LiveBroadcaster(
                () -> {
                    computed.incrementAndGet();
                    return "{}";
                },
                2,
                1_000,
                5);
        try {
            broadcaster.tick();
            broadcaster.tick();
            assertThat(computed).hasValue(0);
        } finally {
            broadcaster.shutdown();
        }
    }

    @Test
    void aFailingSendDisconnectsOnlyThatClient() {
        LiveBroadcaster broadcaster = new LiveBroadcaster(() -> "{}", 5, 1_000, 5);
        try {
            FakeSession ok = new FakeSession("ok", null);
            FakeSession broken = new FakeSession("broken", null);
            broken.failSends = true;
            broadcaster.register(ok);
            broadcaster.register(broken);
            broadcaster.tick();
            await().atMost(Duration.ofSeconds(5)).until(() -> broadcaster.clientCount() == 1);
            broadcaster.tick();
            await().atMost(Duration.ofSeconds(5)).until(() -> ok.received.size() == 2);
        } finally {
            broadcaster.shutdown();
        }
    }

    /** A session whose sends block on {@code gate} (when given) and that records what it receives. */
    static final class FakeSession implements WebSocketSession {
        final String id;
        final CountDownLatch gate;
        final List<String> received = new CopyOnWriteArrayList<>();
        final AtomicInteger sendAttempts = new AtomicInteger();
        final AtomicReference<CloseStatus> closeStatus = new AtomicReference<>();
        volatile boolean failSends;

        FakeSession(String id, CountDownLatch gate) {
            this.id = id;
            this.gate = gate;
        }

        @Override
        public void sendMessage(WebSocketMessage<?> message) throws IOException {
            sendAttempts.incrementAndGet();
            if (failSends) {
                throw new IOException("broken pipe");
            }
            if (gate != null) {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted");
                }
            }
            received.add((String) message.getPayload());
        }

        @Override
        public void close(CloseStatus status) {
            closeStatus.compareAndSet(null, status);
        }

        @Override
        public void close() {
            close(CloseStatus.NORMAL);
        }

        @Override
        public boolean isOpen() {
            return closeStatus.get() == null;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public URI getUri() {
            return URI.create("ws://localhost/ws/v1/live");
        }

        @Override
        public HttpHeaders getHandshakeHeaders() {
            return new HttpHeaders();
        }

        @Override
        public Map<String, Object> getAttributes() {
            return Map.of();
        }

        @Override
        public Principal getPrincipal() {
            return null;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public String getAcceptedProtocol() {
            return null;
        }

        @Override
        public void setTextMessageSizeLimit(int messageSizeLimit) {}

        @Override
        public int getTextMessageSizeLimit() {
            return 0;
        }

        @Override
        public void setBinaryMessageSizeLimit(int messageSizeLimit) {}

        @Override
        public int getBinaryMessageSizeLimit() {
            return 0;
        }

        @Override
        public List<WebSocketExtension> getExtensions() {
            return List.of();
        }
    }
}
