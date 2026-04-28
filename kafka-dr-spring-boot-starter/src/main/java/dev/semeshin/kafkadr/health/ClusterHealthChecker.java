package dev.semeshin.kafkadr.health;

import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        boolean deepProbe = properties.getHealthCheck().isDeepProbe();

        for (Map.Entry<String, ClusterConfig> entry : properties.getClusters().entrySet()) {
            String name = entry.getKey();
            String brokers = entry.getValue().getBootstrapServers();
            Map<String, String> kafkaClientProps = KafkaAdminHelper.extractKafkaClientProperties(
                    properties.getEffectiveEnvironment(name));

            boolean healthy;
            if (deepProbe) {
                healthy = probeDeep(name, brokers, timeout, kafkaClientProps);
            } else {
                healthy = probeAdmin(brokers, timeout, kafkaClientProps);
            }

            clusterManager.reportHealth(name, healthy);
        }
    }

    /**
     * Basic probe: checks cluster metadata via AdminClient.describeCluster().
     * Catches: controller/broker process down, network unreachable.
     * Misses: broker alive but not accepting data (no partition leaders).
     */
    private boolean probeAdmin(String brokers, long timeoutMs, Map<String, String> kafkaClientProps) {
        try (AdminClient admin = KafkaAdminHelper.createAdminClient(brokers, (int) timeoutMs, kafkaClientProps)) {
            admin.describeCluster()
                    .clusterId()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.debug("[{}] Admin probe failed: {}", brokers, e.getMessage());
            return false;
        }
    }

    /**
     * Deep probe: describeCluster + describeTopics to verify partition leaders exist.
     * Catches everything basic probe catches, plus: broker alive but partitions have
     * no leaders (all replicas offline, broker in maintenance, etc.).
     * No data is written — read-only metadata check.
     */
    private boolean probeDeep(String clusterName, String brokers, long timeoutMs,
                              Map<String, String> kafkaClientProps) {
        try (AdminClient admin = KafkaAdminHelper.createAdminClient(brokers, (int) timeoutMs, kafkaClientProps)) {
            // Step 1: cluster must be reachable
            admin.describeCluster()
                    .clusterId()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            // Step 2: check that configured topics have partition leaders
            Set<String> topics = getConfiguredTopics();
            if (topics.isEmpty()) {
                return true;
            }

            // Only check topics that actually exist on this cluster
            Set<String> existingTopics = admin.listTopics()
                    .names()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            topics.retainAll(existingTopics);

            if (topics.isEmpty()) {
                return true;
            }

            Map<String, TopicDescription> descriptions = admin.describeTopics(topics)
                    .allTopicNames()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            int minNodes = properties.getHealthCheck().getDeepProbeMinNodes();
            int minIsr = properties.getHealthCheck().getDeepProbeMinIsr();

            for (Map.Entry<String, TopicDescription> desc : descriptions.entrySet()) {
                String topicName = desc.getKey();
                Set<Integer> topicActiveNodes = new HashSet<>();

                for (TopicPartitionInfo partition : desc.getValue().partitions()) {
                    Node leader = partition.leader();
                    if (leader != null && leader.id() != Node.noNode().id()) {
                        topicActiveNodes.add(leader.id());
                    }

                    if (minIsr > 0 && partition.isr().size() < minIsr) {
                        log.warn("DR_EVENT [{}][{}] Partition {} ISR={}, required {}",
                                clusterName, topicName, partition.partition(),
                                partition.isr().size(), minIsr);
                        return false;
                    }
                }

                if (topicActiveNodes.size() < minNodes) {
                    log.warn("DR_EVENT [{}][{}] Only {} active node(s), required {}",
                            clusterName, topicName, topicActiveNodes.size(), minNodes);
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            log.debug("[{}] Deep probe failed: {}", clusterName, e.getMessage());
            return false;
        }
    }

    private Set<String> getConfiguredTopics() {
        return Stream.concat(
                properties.getConsumers().stream().map(KafkaClusterProperties.ConsumerConfig::getTopic),
                properties.getProducers().stream().map(KafkaClusterProperties.ProducerConfig::getTopic)
        ).collect(Collectors.toSet());
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
