package com.example.kafkadr.health;

import com.example.kafkadr.config.KafkaClusterProperties;
import com.example.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import com.example.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class ClusterHealthChecker implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealthChecker.class);
    private static final String KAFKA_BINDER_CONFIG_PREFIX = "spring.cloud.stream.kafka.binder.configuration.";

    private final KafkaClusterProperties properties;
    private final ActiveClusterManager clusterManager;

    public ClusterHealthChecker(KafkaClusterProperties properties,
                                ActiveClusterManager clusterManager) {
        this.properties = properties;
        this.clusterManager = clusterManager;
    }

    @Scheduled(fixedDelayString = "${kafka-dr.health-check.interval-ms:5000}")
    public void checkAllClusters() {
        long timeout = properties.getHealthCheck().getTimeoutMs();

        for (Map.Entry<String, ClusterConfig> entry : properties.getClusters().entrySet()) {
            String name = entry.getKey();
            String brokers = entry.getValue().getBootstrapServers();
            Map<String, String> kafkaClientProps = extractKafkaClientProperties(name);
            boolean healthy = probe(brokers, timeout, kafkaClientProps);
            clusterManager.reportHealth(name, healthy);
        }
    }

    /**
     * Extracts raw Kafka client properties from the merged environment.
     * Properties under spring.cloud.stream.kafka.binder.configuration.* are Kafka client properties
     * (security.protocol, ssl.*, sasl.*, etc.)
     */
    private Map<String, String> extractKafkaClientProperties(String clusterName) {
        Map<String, String> env = properties.getEffectiveEnvironment(clusterName);
        Map<String, String> kafkaProps = new HashMap<>();

        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (entry.getKey().startsWith(KAFKA_BINDER_CONFIG_PREFIX)) {
                String kafkaKey = entry.getKey().substring(KAFKA_BINDER_CONFIG_PREFIX.length());
                kafkaProps.put(kafkaKey, entry.getValue());
            }
        }

        return kafkaProps;
    }

    private boolean probe(String brokers, long timeoutMs, Map<String, String> kafkaClientProps) {
        Map<String, Object> config = new HashMap<>(kafkaClientProps);
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);

        try (AdminClient admin = AdminClient.create(config)) {
            admin.describeCluster()
                    .clusterId()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.debug("Health probe failed for {}: {}", brokers, e.getMessage());
            return false;
        }
    }

    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        builder.withDetail("activeCluster", clusterManager.getActiveCluster());
        clusterManager.getHealthStatuses()
                .forEach((name, healthy) -> builder.withDetail("cluster." + name, healthy ? "UP" : "DOWN"));
        return builder.build();
    }
}
