package dev.semeshin.kafkadr.health;

import dev.semeshin.kafkadr.config.AdminClientFactory;
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
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ClusterHealthChecker implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealthChecker.class);

    private final KafkaClusterProperties properties;
    private final ActiveClusterManager clusterManager;
    private final AdminClientFactory adminClientFactory;
    private final ExecutorService probeExecutor;

    public ClusterHealthChecker(KafkaClusterProperties properties,
                                ActiveClusterManager clusterManager,
                                AdminClientFactory adminClientFactory) {
        this.properties = properties;
        this.clusterManager = clusterManager;
        this.adminClientFactory = adminClientFactory;
        // One thread per cluster so a slow/unreachable cluster never delays the others.
        int poolSize = Math.max(1, properties.getClusters().size());
        this.probeExecutor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "kafka-dr-health-probe");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        probeExecutor.shutdownNow();
    }

    /**
     * Uses fixedRate (not fixedDelay) so the cadence is wall-clock — the interval is
     * not stretched by how long the probes take. Probes run in parallel and each is
     * bounded by timeout-ms, so a full round can't exceed roughly that bound.
     */
    @Scheduled(fixedRateString = "${kafka-dr.health-check.interval-ms:5000}")
    public void checkAllClusters() {
        long timeout = properties.getHealthCheck().getTimeoutMs();
        boolean deepProbe = properties.getHealthCheck().isDeepProbe();

        // Hard ceiling on how long we wait for a probe result. The deep probe issues up
        // to three sequential admin calls, each bounded by timeout; basic probe just one.
        long awaitMs = (deepProbe ? timeout * 3 : timeout) + 2000;

        Map<String, Future<Boolean>> inFlight = new LinkedHashMap<>();
        for (Map.Entry<String, ClusterConfig> entry : properties.getClusters().entrySet()) {
            String name = entry.getKey();
            String brokers = entry.getValue().getBootstrapServers();
            Map<String, String> kafkaClientProps = KafkaAdminHelper.extractKafkaClientProperties(
                    properties.getEffectiveEnvironment(name));

            inFlight.put(name, probeExecutor.submit(() -> deepProbe
                    ? probeDeep(name, brokers, timeout, kafkaClientProps)
                    : probeAdmin(brokers, timeout, kafkaClientProps)));
        }

        for (Map.Entry<String, Future<Boolean>> entry : inFlight.entrySet()) {
            String name = entry.getKey();
            boolean healthy;
            try {
                healthy = entry.getValue().get(awaitMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                entry.getValue().cancel(true);
                log.debug("[{}] Health probe did not complete within {}ms: {}", name, awaitMs, e.getMessage());
                healthy = false;
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
        try (AdminClient admin = adminClientFactory.create(brokers, (int) timeoutMs, kafkaClientProps)) {
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
        try (AdminClient admin = adminClientFactory.create(brokers, (int) timeoutMs, kafkaClientProps)) {
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
                properties.getConsumers().values().stream().map(KafkaClusterProperties.ConsumerConfig::getTopic),
                properties.getProducers().values().stream().map(KafkaClusterProperties.ProducerConfig::getTopic)
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
