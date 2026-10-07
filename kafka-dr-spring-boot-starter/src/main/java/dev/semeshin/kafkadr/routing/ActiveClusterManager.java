package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.concurrent.DaemonExecutors;
import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Elects the active cluster of every cluster group and moves it on failure and recovery.
 *
 * <p>Each group is elected on its own, with its own thresholds, failback window and persisted
 * state: losing a cluster in one group never touches the active cluster of another. Clusters
 * are addressed by binder id, which is unique across groups, so health reports need no group.
 *
 * <p>State changes of a group are serialized on that group — the health checker and producers
 * forcing a failover report from different threads. Its events are queued in that order and
 * published once the state lock is released, by whichever thread gets there first. Publishing
 * under the lock would stall a failover: a switch stops listener containers synchronously, the
 * stop waits for the listener thread, and that thread may be inside a send to the same group,
 * about to report the very failure. A thread that finds another one publishing leaves its
 * events to it and returns.
 *
 * <p>A failover forced by a producer is published on a dedicated thread of its group, never on
 * the caller's, and never behind another group's container stop.
 * The caller is usually a listener thread sending through {@code ResilientProducer}; publishing
 * there would have it stop its own container, or — with bridges between groups — two listener
 * threads stop each other's, each stop waiting out the container's shutdown timeout. The state
 * changes at once all the same, so the very next send already goes to the new cluster.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ActiveClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ActiveClusterManager.class);

    private final ApplicationEventPublisher eventPublisher;
    private final FailoverStateStore failoverStateStore;
    private final Map<String, GroupState> groups = new LinkedHashMap<>();
    private final Map<String, GroupState> groupByCluster = new LinkedHashMap<>();
    /** How long queued failover side effects may take to finish at shutdown. */
    private static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(2);
    /** Publishes producer-forced failovers when given; null for one owned thread per group. */
    private final Executor injectedPublisher;
    private final Clock clock;
    /** Per-group publisher threads this manager created, shut down with it. */
    private final List<ExecutorService> ownedExecutors = new ArrayList<>();

    /** Resolves the topology itself — for use outside a Spring context. */
    public ActiveClusterManager(KafkaClusterProperties properties,
                                ApplicationEventPublisher eventPublisher,
                                FailoverStateStore failoverStateStore) {
        this(properties.topology(), eventPublisher, failoverStateStore);
    }

    @Autowired
    public ActiveClusterManager(ClusterTopology topology,
                                ApplicationEventPublisher eventPublisher,
                                FailoverStateStore failoverStateStore) {
        this(topology, eventPublisher, failoverStateStore, null, Clock.systemDefaultZone());
    }

    /**
     * @param forcedSwitchPublisher runs the publication of producer-forced failovers; null for a
     *                              daemon thread per group, so groups never queue behind each other
     * @param clock                 time and zone {@code failback-after} is read in
     */
    ActiveClusterManager(ClusterTopology topology,
                         ApplicationEventPublisher eventPublisher,
                         FailoverStateStore failoverStateStore,
                         Executor forcedSwitchPublisher,
                         Clock clock) {
        this.clock = clock;
        this.eventPublisher = eventPublisher;
        this.failoverStateStore = failoverStateStore;
        this.injectedPublisher = forcedSwitchPublisher;

        if (topology.isEmpty()) {
            throw new IllegalStateException(
                    "At least one Kafka cluster must be configured under kafka-dr.clusters or kafka-dr.cluster-groups");
        }
        if (topology.isMultiGroup()) {
            requireGroupAwareStore(failoverStateStore);
        }

        for (ClusterTopology.Group group : topology.groups()) {
            GroupState state = new GroupState(group);
            groups.put(group.name(), state);
            for (String cluster : state.clustersByPriority) {
                groupByCluster.put(cluster, state);
            }
            log.info("{}Cluster priority order: {}, initial: {}", state.logPrefix, state.clustersByPriority,
                    state.activeCluster);
        }
    }

    /**
     * Gives each group's queue a moment to drain before the threads go: a producer-forced failover
     * persists its state on that thread, and dropping it would make the next start elect the
     * cluster that just failed, ignoring failback-after.
     */
    @PreDestroy
    void shutdown() {
        ownedExecutors.forEach(executor -> DaemonExecutors.shutdownGracefully(executor, SHUTDOWN_GRACE));
    }

    private Executor publisherFor(String group) {
        if (injectedPublisher != null) {
            return injectedPublisher;
        }
        ExecutorService executor = DaemonExecutors.singleThread("kafka-dr-failover-events-" + group);
        ownedExecutors.add(executor);
        return executor;
    }

    /**
     * With several groups, a store that only implements the single-state methods would have
     * every group overwrite the others' failover state — and restore a cluster of one group as
     * the active cluster of another after a restart.
     */
    private static void requireGroupAwareStore(FailoverStateStore store) {
        if (!store.supportsGroups()) {
            throw new IllegalStateException(
                    ("%s does not keep failover state per cluster group, but several groups are configured. "
                            + "Implement save(String, FailoverState), load(String) and clear(String) with a key per "
                            + "group, and return true from supportsGroups().").formatted(store.getClass().getName()));
        }
    }

    // --- health reports ---------------------------------------------------------

    /**
     * A health round reports the clusters of a group in priority order. The initial election takes
     * the first healthy report, so reporting in any other order could elect a standby that merely
     * came first — and record that as a failover, held by {@code failback-after}.
     *
     * @param clusterName binder id of the probed cluster
     */
    public void reportHealth(String clusterName, boolean healthy) {
        groupOf(clusterName).reportHealth(clusterName, healthy);
    }

    /**
     * Immediately marks the given cluster as unhealthy and triggers re-election of its group.
     * Called by ResilientProducer when a send failure proves the active cluster is down
     * before the health checker has detected it.
     */
    public void forceUnhealthy(String clusterName) {
        groupOf(clusterName).forceUnhealthy(clusterName);
    }

    // --- queries: per group -------------------------------------------------------

    public String getActiveCluster(String group) {
        return state(group).activeCluster;
    }

    /**
     * True once the group has elected its active cluster and at least one of its clusters is
     * marked healthy. Producers consult this before sending: with no healthy cluster, a real
     * send would only block on the binder's topic provisioning / metadata lookup (max.block.ms)
     * against dead brokers, so the producer fails fast instead.
     *
     * <p>False before the initial election even when a cluster has already reported healthy: a
     * group restored from the {@link FailoverStateStore} waits for its restored cluster's report,
     * and until then the active cluster is one nobody has checked.
     */
    public boolean hasHealthyCluster(String group) {
        return state(group).serving();
    }

    public List<String> getClustersByPriority(String group) {
        return state(group).clustersByPriority;
    }

    public Map<String, Boolean> getHealthStatuses(String group) {
        return Map.copyOf(state(group).healthStatus);
    }

    /** Active cluster of every group, groups in declaration order. */
    public Map<String, String> getActiveClusters() {
        Map<String, String> active = new LinkedHashMap<>();
        groups.forEach((name, state) -> active.put(name, state.activeCluster));
        return active;
    }

    public List<String> getGroups() {
        return List.copyOf(groups.keySet());
    }

    /** Group of the cluster with this binder id. */
    public String getGroupOfCluster(String clusterName) {
        return groupOf(clusterName).name;
    }

    /** Health of every cluster of every group, keyed by binder id. */
    public Map<String, Boolean> getHealthStatuses() {
        Map<String, Boolean> all = new LinkedHashMap<>();
        groups.values().forEach(state -> all.putAll(state.healthStatus));
        return Map.copyOf(all);
    }

    // --- queries: single group ----------------------------------------------------

    /**
     * Active cluster of the only cluster group.
     *
     * @throws IllegalStateException when several groups are configured — use
     *                               {@link #getActiveCluster(String)} or {@link #getActiveClusters()}
     */
    public String getActiveCluster() {
        return onlyGroup("getActiveCluster(group)").activeCluster;
    }

    /**
     * @throws IllegalStateException when several groups are configured — use
     *                               {@link #hasHealthyCluster(String)}
     */
    public boolean hasHealthyCluster() {
        return onlyGroup("hasHealthyCluster(group)").serving();
    }

    /**
     * @throws IllegalStateException when several groups are configured — use
     *                               {@link #getClustersByPriority(String)}
     */
    public List<String> getClustersByPriority() {
        return onlyGroup("getClustersByPriority(group)").clustersByPriority;
    }

    private GroupState onlyGroup(String alternative) {
        if (groups.size() != 1) {
            throw new IllegalStateException(
                    "%d cluster groups are configured %s — each has its own active cluster. Use %s."
                            .formatted(groups.size(), groups.keySet(), alternative));
        }
        return groups.values().iterator().next();
    }

    private GroupState state(String group) {
        GroupState state = groups.get(group);
        if (state == null) {
            throw new IllegalArgumentException("Unknown cluster group '%s'. Known groups: %s"
                    .formatted(group, groups.keySet()));
        }
        return state;
    }

    private GroupState groupOf(String clusterName) {
        GroupState state = groupByCluster.get(clusterName);
        if (state == null) {
            throw new IllegalArgumentException("Unknown cluster '%s'. Known clusters: %s"
                    .formatted(clusterName, groupByCluster.keySet()));
        }
        return state;
    }

    // --- one group ------------------------------------------------------------------

    /** A side effect of a state change, carried out outside the group's lock. */
    private record Action(String what, Runnable run) {}

    /** Election state of one cluster group. */
    private final class GroupState {

        private final String name;
        /** Prefix naming the group in log lines; empty for a single group, as before groups existed. */
        private final String logPrefix;
        private final KafkaClusterProperties.HealthCheckConfig healthCheck;
        private final KafkaClusterProperties.FailoverConfig failover;

        /** Sorted list of cluster ids by priority (lowest priority value = first) */
        private final List<String> clustersByPriority;

        private volatile String activeCluster;
        private volatile boolean failoverOccurred = false;
        private volatile Instant failoverAt;

        private final ConcurrentHashMap<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> successCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Boolean> healthStatus = new ConcurrentHashMap<>();
        private volatile boolean initialElectionDone = false;
        /** Last availability published, so the event fires on transitions only. */
        private boolean available = false;
        /**
         * Cluster restored from the FailoverStateStore, still within its failback window; null when
         * nothing was restored. It wins the initial election if it is healthy — immediately, without
         * waiting out recovery-threshold, and ahead of a higher-priority cluster.
         */
        private String restored;
        /** Clusters that have reported at least once; the initial election waits for the restored one. */
        private final Set<String> reported = ConcurrentHashMap.newKeySet();
        /** Runs the publication of this group's producer-forced failovers. */
        private final Executor forcedSwitchPublisher;
        /**
         * Side effects of state changes — events to publish, failover state to persist — in the
         * order the state changed, carried out once the lock is released. Persisting may be remote
         * I/O (Redis, say); doing it under the lock would make every report of the group, and every
         * send forcing a failover, wait on the store.
         */
        private final Queue<Action> pending = new ConcurrentLinkedQueue<>();
        private final ReentrantLock publishing = new ReentrantLock();

        private GroupState(ClusterTopology.Group group) {
            this.name = group.name();
            this.logPrefix = group.isDefault() ? "" : "[" + group.name() + "] ";
            this.healthCheck = group.healthCheck();
            this.failover = group.failover();
            this.clustersByPriority = group.clustersByPriority().stream()
                    .map(ClusterTopology.ClusterRef::id)
                    .toList();
            this.forcedSwitchPublisher = publisherFor(group.name());

            for (String id : clustersByPriority) {
                failureCounts.put(id, new AtomicInteger(0));
                successCounts.put(id, new AtomicInteger(0));
                healthStatus.put(id, false);
            }

            this.activeCluster = restoreOrDefaultActive();
        }

        private String restoreOrDefaultActive() {
            String defaultCluster = clustersByPriority.get(0);
            Optional<FailoverStateStore.FailoverState> stored = failoverStateStore.load(name);
            if (stored.isEmpty()) {
                return defaultCluster;
            }

            FailoverStateStore.FailoverState state = stored.get();
            if (!clustersByPriority.contains(state.activeCluster())) {
                log.warn("DR_EVENT {}Persisted active cluster [{}] not in current config — clearing",
                        logPrefix, state.activeCluster());
                failoverStateStore.clear(name);
                return defaultCluster;
            }

            Optional<Instant> threshold = computeFailbackThreshold(state.failoverAt());
            if (threshold.isEmpty()) {
                log.info("DR_EVENT {}failback-after not configured — clearing persisted state", logPrefix);
                failoverStateStore.clear(name);
                return defaultCluster;
            }

            if (!clock.instant().isBefore(threshold.get())) {
                log.info("DR_EVENT {}Persisted failover at {} past failback threshold {} — clearing",
                        logPrefix, state.failoverAt(), threshold.get());
                failoverStateStore.clear(name);
                return defaultCluster;
            }

            log.warn("DR_EVENT {}Restoring active cluster [{}] from persisted failover at {} (failback allowed after {})",
                    logPrefix, state.activeCluster(), state.failoverAt(), threshold.get());
            this.failoverOccurred = true;
            this.failoverAt = state.failoverAt();
            // The initial election still runs — it is what starts the bindings — but it waits for
            // this cluster's report and prefers it, so the failback window is honoured.
            this.restored = state.activeCluster();
            return state.activeCluster();
        }

        private Optional<Instant> computeFailbackThreshold(Instant failoverAt) {
            String failbackAfter = failover.getFailbackAfter();
            if (failbackAfter == null || failbackAfter.isBlank()) {
                return Optional.empty();
            }
            LocalTime time = LocalTime.parse(failbackAfter);
            ZoneId zone = clock.getZone();
            ZonedDateTime sameDay = failoverAt.atZone(zone).with(time);
            ZonedDateTime threshold = sameDay.toInstant().isBefore(failoverAt)
                    ? sameDay.plusDays(1)
                    : sameDay;
            return Optional.of(threshold.toInstant());
        }

        void reportHealth(String clusterName, boolean healthy) {
            updateHealth(clusterName, healthy);
            publishPending();
        }

        void forceUnhealthy(String clusterName) {
            forceUnhealthyUnderLock(clusterName);
            if (pending.isEmpty()) {
                return;
            }
            try {
                forcedSwitchPublisher.execute(this::publishPending);
            } catch (RejectedExecutionException e) {
                // The manager is shutting down. The state has moved and nothing will consume
                // after the context closes; a send must still end in a result, not this exception.
                log.debug("DR_EVENT {}Shutting down — switch of group '{}' not published", logPrefix, name);
            }
        }

        private synchronized void updateHealth(String clusterName, boolean healthy) {
            int failureThreshold = healthCheck.getFailureThreshold();
            int recoveryThreshold = healthCheck.getRecoveryThreshold();
            reported.add(clusterName);

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

                if (healthStatus.get(clusterName)) {
                    failBackOnceAllowed(clusterName);
                } else {
                    // Skip recovery threshold on initial startup — elect immediately
                    if (!initialElectionDone) {
                        healthStatus.put(clusterName, true);
                        log.info("DR_EVENT [{}] Marked HEALTHY (initial)", clusterName);
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
            if (!initialElectionDone && hasHealthyCluster()
                    && (restored == null || reported.contains(restored))) {
                reelectActive();
                initialElectionDone = true;
            }
            publishAvailabilityChange();
        }

        private synchronized void forceUnhealthyUnderLock(String clusterName) {
            if (healthStatus.getOrDefault(clusterName, false)) {
                healthStatus.put(clusterName, false);
                failureCounts.get(clusterName).set(healthCheck.getFailureThreshold());
                successCounts.get(clusterName).set(0);
                log.warn("DR_EVENT [{}] Force-marked UNHEALTHY by producer", clusterName);
                reelectActive();
                publishAvailabilityChange();
            }
        }

        boolean hasHealthyCluster() {
            return healthStatus.containsValue(true);
        }

        /** Whether the group can take traffic: elected, and with a healthy cluster. */
        boolean serving() {
            return initialElectionDone && hasHealthyCluster();
        }

        /**
         * A failback held by {@code failback-after} has no health transition of its own to trigger
         * it: the preferred cluster turned healthy long before the window opened. Its later healthy
         * reports are what notice that the window has opened.
         */
        private void failBackOnceAllowed(String clusterName) {
            if (initialElectionDone && failoverOccurred
                    && clustersByPriority.indexOf(clusterName) < clustersByPriority.indexOf(activeCluster)
                    && !isFailbackBlocked()) {
                reelectActive();
            }
        }

        private void reelectActive() {
            String previous = activeCluster;
            boolean previousHealthy = healthStatus.getOrDefault(previous, false);

            if (!initialElectionDone && restored != null && healthStatus.getOrDefault(restored, false)) {
                // Initial election of a restored group: the persisted cluster is healthy, so it stays
                // active and the persisted failover — with its failback window — stays as it is.
                activeCluster = restored;
                log.warn("DR_EVENT [{}] -> [{}] CLUSTER SWITCH (restored)", previous, restored);
                publish(new ClusterSwitchedEvent(ActiveClusterManager.this, name, previous, restored));
                return;
            }

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
                            previous, candidate, failover.getFailbackAfter());
                    return;
                }

                activeCluster = candidate;
                // Electing the preferred cluster at startup is no failover: there is nothing to
                // hold for failback-after, and a stale state from an earlier run must go.
                boolean preferredAtStartup = !initialElectionDone && clustersByPriority.indexOf(candidate) == 0;
                if (!candidate.equals(previous) || !initialElectionDone) {
                    if (isFailback || preferredAtStartup) {
                        failoverOccurred = false;
                        failoverAt = null;
                        pending.add(new Action("clearing failover state", () -> failoverStateStore.clear(name)));
                    } else {
                        Instant now = clock.instant();
                        failoverOccurred = true;
                        failoverAt = now;
                        FailoverStateStore.FailoverState state = new FailoverStateStore.FailoverState(candidate, now);
                        pending.add(new Action("saving failover state", () -> failoverStateStore.save(name, state)));
                    }
                    log.warn("DR_EVENT [{}] -> [{}] CLUSTER SWITCH{}", previous, candidate,
                            isFailback ? " (failback)" : "");
                    publish(new ClusterSwitchedEvent(ActiveClusterManager.this, name, previous, candidate));
                }
                return;
            }

            log.error("DR_EVENT {}[{}] ALL CLUSTERS UNHEALTHY — staying", logPrefix, activeCluster);
        }

        private boolean isFailbackBlocked() {
            Instant at = failoverAt;
            if (at == null) {
                return false;
            }
            Optional<Instant> threshold = computeFailbackThreshold(at);
            return threshold.isPresent() && clock.instant().isBefore(threshold.get());
        }

        private void publishAvailabilityChange() {
            boolean now = serving();
            if (now == available) {
                return;
            }
            available = now;
            if (now) {
                log.info("DR_EVENT {}Cluster group '{}' AVAILABLE", logPrefix, name);
            } else {
                log.error("DR_EVENT {}Cluster group '{}' UNAVAILABLE — no healthy cluster", logPrefix, name);
            }
            publish(new ClusterGroupAvailabilityEvent(ActiveClusterManager.this, name, now));
        }

        private void publish(ApplicationEvent event) {
            pending.add(new Action(event.getClass().getSimpleName(), () -> eventPublisher.publishEvent(event)));
        }

        /**
         * Publishes the queued events in order. Called without the state lock; a thread that finds
         * another one publishing returns at once — the publisher drains its events too, and
         * re-checks the queue after releasing, so nothing is left behind.
         */
        private void publishPending() {
            while (!pending.isEmpty()) {
                if (!publishing.tryLock()) {
                    return;
                }
                try {
                    Action action;
                    while ((action = pending.poll()) != null) {
                        try {
                            action.run().run();
                        } catch (RuntimeException e) {
                            // One failing listener or store call must not hold back what is queued behind it.
                            log.error("DR_EVENT {}Failed {}", logPrefix, action.what(), e);
                        }
                    }
                } finally {
                    publishing.unlock();
                }
            }
        }
    }
}
