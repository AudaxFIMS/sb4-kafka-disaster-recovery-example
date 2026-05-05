package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ActiveClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ActiveClusterManager.class);

    private final KafkaClusterProperties properties;
    private final ApplicationEventPublisher eventPublisher;
    private final FailoverStateStore failoverStateStore;

    /** Sorted list of cluster names by priority (lowest priority value = first) */
    private final List<String> clustersByPriority;

    private volatile String activeCluster;
    private volatile boolean failoverOccurred = false;
    private volatile Instant failoverAt;

    private final ConcurrentHashMap<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> successCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> healthStatus = new ConcurrentHashMap<>();
    private volatile boolean initialElectionDone = false;

    public ActiveClusterManager(KafkaClusterProperties properties,
                                ApplicationEventPublisher eventPublisher,
                                FailoverStateStore failoverStateStore) {
        this.properties = properties;
        this.eventPublisher = eventPublisher;
        this.failoverStateStore = failoverStateStore;

        this.clustersByPriority = properties.getClusters().entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getValue().getPriority()))
                .map(Map.Entry::getKey)
                .toList();

        if (clustersByPriority.isEmpty()) {
            throw new IllegalStateException("At least one Kafka cluster must be configured under kafka-dr.clusters");
        }

        for (String name : clustersByPriority) {
            failureCounts.put(name, new AtomicInteger(0));
            successCounts.put(name, new AtomicInteger(0));
            healthStatus.put(name, false);
        }

        this.activeCluster = restoreOrDefaultActive();

        log.info("Cluster priority order: {}, initial: {}", clustersByPriority, activeCluster);
    }

    private String restoreOrDefaultActive() {
        String defaultCluster = clustersByPriority.get(0);
        Optional<FailoverStateStore.FailoverState> stored = failoverStateStore.load();
        if (stored.isEmpty()) {
            return defaultCluster;
        }

        FailoverStateStore.FailoverState state = stored.get();
        if (!clustersByPriority.contains(state.activeCluster())) {
            log.warn("DR_EVENT Persisted active cluster [{}] not in current config — clearing", state.activeCluster());
            failoverStateStore.clear();
            return defaultCluster;
        }

        Optional<Instant> threshold = computeFailbackThreshold(state.failoverAt());
        if (threshold.isEmpty()) {
            log.info("DR_EVENT failback-after not configured — clearing persisted state");
            failoverStateStore.clear();
            return defaultCluster;
        }

        if (!Instant.now().isBefore(threshold.get())) {
            log.info("DR_EVENT Persisted failover at {} past failback threshold {} — clearing",
                    state.failoverAt(), threshold.get());
            failoverStateStore.clear();
            return defaultCluster;
        }

        log.warn("DR_EVENT Restoring active cluster [{}] from persisted failover at {} (failback allowed after {})",
                state.activeCluster(), state.failoverAt(), threshold.get());
        this.failoverOccurred = true;
        this.failoverAt = state.failoverAt();
        this.initialElectionDone = true;
        return state.activeCluster();
    }

    private Optional<Instant> computeFailbackThreshold(Instant failoverAt) {
        String failbackAfter = properties.getFailover().getFailbackAfter();
        if (failbackAfter == null || failbackAfter.isBlank()) {
            return Optional.empty();
        }
        LocalTime time = LocalTime.parse(failbackAfter);
        ZoneId zone = ZoneId.systemDefault();
        ZonedDateTime sameDay = failoverAt.atZone(zone).with(time);
        ZonedDateTime threshold = sameDay.toInstant().isBefore(failoverAt)
                ? sameDay.plusDays(1)
                : sameDay;
        return Optional.of(threshold.toInstant());
    }

    public void reportHealth(String clusterName, boolean healthy) {
        int failureThreshold = properties.getHealthCheck().getFailureThreshold();
        int recoveryThreshold = properties.getHealthCheck().getRecoveryThreshold();

        if (!healthy) {
            successCounts.get(clusterName).set(0);
            int failures = failureCounts.get(clusterName).incrementAndGet();
            log.warn("DR_EVENT [{}] Health check failed ({}/{})", clusterName, failures, failureThreshold);

            if (failures >= failureThreshold && healthStatus.get(clusterName)) {
                healthStatus.put(clusterName, false);
                log.warn("DR_EVENT [{}] Marked UNHEALTHY", clusterName);
                reelectActive();
            }
        } else {
            failureCounts.get(clusterName).set(0);

            if (!healthStatus.get(clusterName)) {
                // Skip recovery threshold on initial startup — elect immediately
                if (!initialElectionDone) {
                    healthStatus.put(clusterName, true);
                    log.info("DR_EVENT [{}] Marked HEALTHY (initial)", clusterName);
                    reelectActive();
                    initialElectionDone = true;
                } else {
                    int successes = successCounts.get(clusterName).incrementAndGet();
                    log.info("DR_EVENT [{}] Recovery check passed ({}/{})", clusterName, successes, recoveryThreshold);

                    if (successes >= recoveryThreshold) {
                        healthStatus.put(clusterName, true);
                        log.info("DR_EVENT [{}] Marked HEALTHY", clusterName);
                        reelectActive();
                    }
                }
            }
        }
    }

    private void reelectActive() {
        String previous = activeCluster;
        boolean previousHealthy = healthStatus.getOrDefault(previous, false);

        for (String candidate : clustersByPriority) {
            if (!healthStatus.getOrDefault(candidate, false)) {
                continue;
            }

            if (candidate.equals(previous) && initialElectionDone) {
                return;
            }

            boolean isFailback = clustersByPriority.indexOf(candidate) < clustersByPriority.indexOf(previous);

            // Block ANY failback while current cluster is healthy and time gate is active
            if (initialElectionDone && isFailback && previousHealthy
                    && failoverOccurred && isFailbackBlocked()) {
                log.debug("DR_EVENT [{}] Failback to [{}] blocked until {}",
                        previous, candidate, properties.getFailover().getFailbackAfter());
                return;
            }

            activeCluster = candidate;
            if (!candidate.equals(previous) || !initialElectionDone) {
                if (isFailback) {
                    failoverOccurred = false;
                    failoverAt = null;
                    failoverStateStore.clear();
                } else {
                    Instant now = Instant.now();
                    failoverOccurred = true;
                    failoverAt = now;
                    failoverStateStore.save(new FailoverStateStore.FailoverState(candidate, now));
                }
                log.warn("DR_EVENT [{}] -> [{}] CLUSTER SWITCH{}", previous, candidate,
                        isFailback ? " (failback)" : "");
                eventPublisher.publishEvent(new ClusterSwitchedEvent(this, previous, candidate));
            }
            return;
        }

        log.error("DR_EVENT [{}] ALL CLUSTERS UNHEALTHY — staying", activeCluster);
    }

    private boolean isFailbackBlocked() {
        Instant at = failoverAt;
        if (at == null) {
            return false;
        }
        Optional<Instant> threshold = computeFailbackThreshold(at);
        return threshold.isPresent() && Instant.now().isBefore(threshold.get());
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
            log.warn("DR_EVENT [{}] Force-marked UNHEALTHY by producer", clusterName);
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
