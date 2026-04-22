package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ActiveClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ActiveClusterManager.class);

    private final KafkaClusterProperties properties;
    private final ApplicationEventPublisher eventPublisher;

    /** Sorted list of cluster names by priority (lowest priority value = first) */
    private final List<String> clustersByPriority;

    private volatile String activeCluster;

    private final ConcurrentHashMap<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> successCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> healthStatus = new ConcurrentHashMap<>();
    private volatile boolean initialElectionDone = false;

    public ActiveClusterManager(KafkaClusterProperties properties,
                                ApplicationEventPublisher eventPublisher) {
        this.properties = properties;
        this.eventPublisher = eventPublisher;

        this.clustersByPriority = properties.getClusters().entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getValue().getPriority()))
                .map(Map.Entry::getKey)
                .toList();

        if (clustersByPriority.isEmpty()) {
            throw new IllegalStateException("At least one Kafka cluster must be configured under kafka-dr.clusters");
        }

        this.activeCluster = clustersByPriority.get(0);

        for (String name : clustersByPriority) {
            failureCounts.put(name, new AtomicInteger(0));
            successCounts.put(name, new AtomicInteger(0));
            healthStatus.put(name, false);
        }

        log.info("Cluster priority order: {}, initial: {}", clustersByPriority, activeCluster);
    }

    public void reportHealth(String clusterName, boolean healthy) {
        int failureThreshold = properties.getHealthCheck().getFailureThreshold();
        int recoveryThreshold = properties.getHealthCheck().getRecoveryThreshold();

        if (!healthy) {
            successCounts.get(clusterName).set(0);
            int failures = failureCounts.get(clusterName).incrementAndGet();
            log.warn("Cluster '{}' health check failed ({}/{})", clusterName, failures, failureThreshold);

            if (failures >= failureThreshold && healthStatus.get(clusterName)) {
                healthStatus.put(clusterName, false);
                log.warn(">>> Cluster '{}' marked UNHEALTHY <<<", clusterName);
                reelectActive();
            }
        } else {
            failureCounts.get(clusterName).set(0);

            if (!healthStatus.get(clusterName)) {
                // Skip recovery threshold on initial startup — elect immediately
                if (!initialElectionDone) {
                    healthStatus.put(clusterName, true);
                    log.info(">>> Cluster '{}' marked HEALTHY (initial) <<<", clusterName);
                    reelectActive();
                    initialElectionDone = true;
                } else {
                    int successes = successCounts.get(clusterName).incrementAndGet();
                    log.info("Cluster '{}' recovery check passed ({}/{})", clusterName, successes, recoveryThreshold);

                    if (successes >= recoveryThreshold) {
                        healthStatus.put(clusterName, true);
                        log.info(">>> Cluster '{}' marked HEALTHY <<<", clusterName);
                        reelectActive();
                    }
                }
            }
        }
    }

    private void reelectActive() {
        String previous = activeCluster;

        for (String candidate : clustersByPriority) {
            if (healthStatus.getOrDefault(candidate, false)) {
                activeCluster = candidate;
                if (!candidate.equals(previous) || !initialElectionDone) {
                    log.warn(">>> CLUSTER SWITCH: '{}' -> '{}' <<<", previous, candidate);
                    eventPublisher.publishEvent(new ClusterSwitchedEvent(this, previous, candidate));
                }
                return;
            }
        }

        log.error(">>> ALL CLUSTERS UNHEALTHY — staying on '{}' <<<", activeCluster);
    }

    /**
     * Immediately marks the given cluster as unhealthy and triggers re-election.
     * Called by ResilientProducer when a send failure proves the active cluster is down
     * before the health checker has detected it.
     */
    public void forceUnhealthy(String clusterName) {
        if (healthStatus.getOrDefault(clusterName, false)) {
            healthStatus.put(clusterName, false);
            failureCounts.get(clusterName).set(properties.getHealthCheck().getFailureThreshold());
            successCounts.get(clusterName).set(0);
            log.warn(">>> Cluster '{}' force-marked UNHEALTHY by producer <<<", clusterName);
            reelectActive();
        }
    }

    public String getActiveCluster() {
        return activeCluster;
    }

    public List<String> getClustersByPriority() {
        return clustersByPriority;
    }

    public Map<String, Boolean> getHealthStatuses() {
        return Map.copyOf(healthStatus);
    }
}
