package io.github.gladwynl.streamflow.kafka;

import io.github.gladwynl.streamflow.config.StreamflowProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
class KafkaAdminConfiguration {

    /**
     * A long-lived admin client shared by topic provisioning, status and health checks. It reuses the connection
     * settings of Spring Boot's {@link KafkaAdmin} and bounds every request by the configured admin timeout.
     */
    @Bean(destroyMethod = "close")
    Admin streamflowAdmin(KafkaAdmin kafkaAdmin, StreamflowProperties properties) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        int timeoutMs = Math.toIntExact(properties.kafka().adminTimeout().toMillis());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        config.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "streamflow-admin");
        return Admin.create(config);
    }
}
