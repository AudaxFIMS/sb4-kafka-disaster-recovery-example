package dev.semeshin.kafkadr.health;

import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ClusterHealthChecker implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealthChecker.class);

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
            Map<String, String> kafkaClientProps = KafkaAdminHelper.extractKafkaClientProperties(
                    properties.getEffectiveEnvironment(name));
            boolean healthy = probe(brokers, timeout, kafkaClientProps);
            clusterManager.reportHealth(name, healthy);
        }
    }

private boolean probe(String brokers, long timeoutMs, Map<String, String> kafkaClientProps) {
        try (AdminClient admin = KafkaAdminHelper.createAdminClient(brokers, (int) timeoutMs, kafkaClientProps)) {
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
