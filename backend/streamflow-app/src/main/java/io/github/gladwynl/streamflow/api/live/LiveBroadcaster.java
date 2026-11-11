package io.github.gladwynl.streamflow.api.live;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * Pushes one snapshot per second to every connected dashboard. This is the only path from the api process to
 * browsers, and it is built so that a slow or stalled browser affects nobody else:
 *
 * <ul>
 *   <li>the snapshot is computed once per tick and serialized once, whatever the number of clients;
 *   <li>sends run on a small bounded pool, never on the ticking thread;
 *   <li>a client whose previous snapshot is still being sent skips the tick instead of queueing it (each client
 *       has at most one snapshot in flight), and is disconnected after too many consecutive skips;
 *   <li>a send that exceeds the send timeout disconnects the client;
 *   <li>the number of clients is capped.
 * </ul>
 *
 * Kafka processing runs in the worker processes and never waits on this class.
 */
public class LiveBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(LiveBroadcaster.class);

    /** Closes a client that cannot keep up. */
    public static final CloseStatus TOO_SLOW = new CloseStatus(4008, "client too slow");

    /** Refuses a connection beyond the client limit (1013: try again later). */
    public static final CloseStatus TOO_MANY_CLIENTS = new CloseStatus(1013, "too many live clients");

    private static final int BUFFER_LIMIT_BYTES = 1024 * 1024;

    static final class Client {
        final WebSocketSession session;
        final AtomicBoolean sending = new AtomicBoolean();
        final AtomicInteger consecutiveSkips = new AtomicInteger();

        Client(WebSocketSession session) {
            this.session = session;
        }
    }

    private final Supplier<String> snapshotJson;
    private final int maxClients;
    private final int sendTimeoutMillis;
    private final int maxSkippedTicks;
    private final Map<String, Client> clients = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor senders;
    private final ScheduledExecutorService ticker;
    private final AtomicLong ticks = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong closedSlow = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    public LiveBroadcaster(Supplier<String> snapshotJson, StreamflowProperties.Api settings) {
        this(
                snapshotJson,
                settings.maxLiveClients(),
                (int) settings.liveSendTimeout().toMillis(),
                settings.liveMaxSkippedTicks());
    }

    LiveBroadcaster(Supplier<String> snapshotJson, int maxClients, int sendTimeoutMillis, int maxSkippedTicks) {
        this.snapshotJson = snapshotJson;
        this.maxClients = maxClients;
        this.sendTimeoutMillis = sendTimeoutMillis;
        this.maxSkippedTicks = maxSkippedTicks;
        this.senders = new ThreadPoolExecutor(
                4, 4, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(Math.max(1, maxClients)), daemon("live-send"));
        this.senders.allowCoreThreadTimeOut(true);
        this.ticker = Executors.newSingleThreadScheduledExecutor(daemon("live-tick"));
    }

    /** Starts ticking once per second. */
    public void start() {
        ticker.scheduleWithFixedDelay(this::tickSafely, 1, 1, TimeUnit.SECONDS);
    }

    /** Registers a connected client, or closes it if the client limit is reached. */
    public boolean register(WebSocketSession session) {
        synchronized (clients) {
            if (clients.size() >= maxClients) {
                refused.incrementAndGet();
                closeQuietly(session, TOO_MANY_CLIENTS);
                return false;
            }
            WebSocketSession decorated = new ConcurrentWebSocketSessionDecorator(
                    session,
                    sendTimeoutMillis,
                    BUFFER_LIMIT_BYTES,
                    ConcurrentWebSocketSessionDecorator.OverflowStrategy.DROP);
            clients.put(session.getId(), new Client(decorated));
        }
        return true;
    }

    public void unregister(WebSocketSession session) {
        clients.remove(session.getId());
    }

    public int clientCount() {
        return clients.size();
    }

    private void tickSafely() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("Live snapshot tick failed", e);
        }
    }

    /** One broadcast round. Does nothing (and computes nothing) while no client is connected. */
    void tick() {
        if (clients.isEmpty()) {
            return;
        }
        ticks.incrementAndGet();
        TextMessage message = new TextMessage(snapshotJson.get());
        for (Client client : clients.values()) {
            if (!client.sending.compareAndSet(false, true)) {
                skip(client);
                continue;
            }
            try {
                senders.execute(() -> send(client, message));
            } catch (RejectedExecutionException e) {
                client.sending.set(false);
                skip(client);
            }
        }
    }

    private void skip(Client client) {
        skipped.incrementAndGet();
        if (client.consecutiveSkips.incrementAndGet() >= maxSkippedTicks) {
            closedSlow.incrementAndGet();
            clients.remove(client.session.getId());
            // Closing must not block the ticker on the stalled connection.
            Thread.ofVirtual().name("live-close").start(() -> closeQuietly(client.session, TOO_SLOW));
        }
    }

    private void send(Client client, TextMessage message) {
        try {
            if (client.session.isOpen()) {
                client.session.sendMessage(message);
                sent.incrementAndGet();
                client.consecutiveSkips.set(0);
            } else {
                clients.remove(client.session.getId());
            }
        } catch (IOException | RuntimeException e) {
            // Includes SessionLimitExceededException: the decorator already closed the session.
            log.debug("Dropping live client {}: {}", client.session.getId(), e.toString());
            clients.remove(client.session.getId());
            closeQuietly(client.session, TOO_SLOW);
        } finally {
            client.sending.set(false);
        }
    }

    public Stats stats() {
        return new Stats(clients.size(), ticks.get(), sent.get(), skipped.get(), closedSlow.get(), refused.get());
    }

    public record Stats(int clients, long ticks, long sent, long skipped, long closedSlow, long refused) {}

    @PreDestroy
    public void shutdown() {
        ticker.shutdownNow();
        clients.values().forEach(c -> closeQuietly(c.session, CloseStatus.GOING_AWAY));
        clients.clear();
        senders.shutdownNow();
    }

    private static void closeQuietly(WebSocketSession session, CloseStatus status) {
        try {
            if (session.isOpen()) {
                session.close(status);
            }
        } catch (IOException | RuntimeException e) {
            log.debug("Closing live client {} failed: {}", session.getId(), e.toString());
        }
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        AtomicInteger n = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, name + "-" + n.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
