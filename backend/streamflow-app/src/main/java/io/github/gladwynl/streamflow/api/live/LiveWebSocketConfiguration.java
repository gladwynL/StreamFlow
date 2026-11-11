package io.github.gladwynl.streamflow.api.live;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.NativeWebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.json.JsonMapper;

/**
 * The live feed endpoint {@value #PATH}. Browsers only receive; anything a client sends is ignored apart from
 * keeping the connection alive, and inbound messages are limited to a few kilobytes. Connections are accepted only
 * from localhost origins.
 */
@Configuration
@EnableWebSocket
@Profile("api")
class LiveWebSocketConfiguration implements WebSocketConfigurer {

    static final String PATH = "/ws/v1/live";

    private final LiveSnapshotService snapshots;
    private final JsonMapper json;
    private final StreamflowProperties properties;

    LiveWebSocketConfiguration(LiveSnapshotService snapshots, JsonMapper json, StreamflowProperties properties) {
        this.snapshots = snapshots;
        this.json = json;
        this.properties = properties;
    }

    @Bean(initMethod = "start")
    LiveBroadcaster liveBroadcaster() {
        return new LiveBroadcaster(
                () -> json.writeValueAsString(Map.of("type", "snapshot", "data", snapshots.snapshot())),
                properties.api());
    }

    /**
     * Per-connection limits: small inbound messages only, and a connection silent for 45 s is dead (the browser
     * pings every 15 s). Applied per session so no servlet-container bean is needed.
     */
    static void limit(WebSocketSession session) {
        session.setTextMessageSizeLimit(4 * 1024);
        session.setBinaryMessageSizeLimit(1024);
        if (session instanceof NativeWebSocketSession nativeSession) {
            jakarta.websocket.Session standard = nativeSession.getNativeSession(jakarta.websocket.Session.class);
            if (standard != null) {
                standard.setMaxIdleTimeout(45_000L);
            }
        }
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        LiveBroadcaster broadcaster = liveBroadcaster();
        registry.addHandler(
                        new TextWebSocketHandler() {
                            @Override
                            public void afterConnectionEstablished(WebSocketSession session) {
                                limit(session);
                                broadcaster.register(session);
                            }

                            @Override
                            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                                // Client pings only keep the connection alive.
                            }

                            @Override
                            public void handleTransportError(WebSocketSession session, Throwable exception) {
                                broadcaster.unregister(session);
                            }

                            @Override
                            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                                broadcaster.unregister(session);
                            }
                        },
                        PATH)
                .setAllowedOriginPatterns("http://localhost:[*]", "http://127.0.0.1:[*]");
    }
}
