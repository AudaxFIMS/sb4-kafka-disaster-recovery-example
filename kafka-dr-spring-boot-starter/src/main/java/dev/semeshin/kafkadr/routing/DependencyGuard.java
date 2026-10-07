package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.concurrent.DaemonExecutors;
import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.DependencyGate;
import dev.semeshin.kafkadr.producer.ClusterGroupUnavailableException;
import dev.semeshin.kafkadr.producer.SendFailedException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Carries out {@code depends-on}: while a cluster group a consumer depends on has no healthy
 * cluster, that consumer's binding is paused; once every dependency is back, it is resumed.
 *
 * <p>Pausing keeps the consumer in its group with its partitions, so nothing is rebalanced and
 * nothing is lost — the records simply wait in Kafka. Records already polled when the pause
 * takes effect are held back by the consumer itself, through the {@link DependencyGate} this
 * guard hands out. The gate is also what catches the moment a send fails before any health
 * check has noticed: the producer marks the clusters unhealthy as it fails, so by the time the
 * handler's exception arrives the dependency is already reported down.
 *
 * <p>All state changes are serialized here. The availability of one group and the switch of
 * another are reported from different threads, and a gate may ask to pause from a consumer thread
 * at any time; deciding under one lock is what keeps a resume from being undone by a pause based
 * on a moment-old view.
 *
 * <p>A consumer thread never touches a binding, nor waits for that lock. Pausing a binding takes
 * the binding's own monitor, which a concurrent stop of the same binding holds while it waits for
 * the consumer thread — so a consumer thread pausing, or waiting for someone who is, would close a
 * cycle that only the container's shutdown timeout breaks. A gate therefore only asks for the
 * pause; the guard's own thread carries it out, re-checking first, while the record is held back.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class DependencyGuard {

    private static final Logger log = LoggerFactory.getLogger(DependencyGuard.class);

    private final ActiveClusterManager clusterManager;
    private final BindingLifecycleManager bindingLifecycleManager;
    /** Consumers with dependencies, by name. */
    private final Map<String, Dependent> dependents = new LinkedHashMap<>();
    /** Cluster whose bindings were last started, per group — where a pause has to land. */
    private final Map<String, String> startedClusterByGroup = new HashMap<>();
    /** Cluster on which each paused consumer is paused. */
    private final Map<String, String> pausedOn = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    /** Carries out pauses a gate asked for — never on the consumer thread that asked. */
    private final Executor pauseRequests;
    /** The guard's own thread when it created it, shut down with the guard. */
    private final ExecutorService ownedExecutor;
    private final Clock clock;
    /**
     * Per consumer, then per partition: the record currently held back and how long it has been
     * held while its dependencies were up. Shared by every gate of the consumer.
     */
    private final Map<String, Map<String, Hold>> holds = new ConcurrentHashMap<>();
    /** Consumers with a pause request queued, so a burst of held-back records queues one. */
    private final Set<String> pauseRequested = ConcurrentHashMap.newKeySet();

    private record Dependent(String name, String group, List<String> dependsOn, Duration nackInterval,
                             Duration maxHold) {}

    /** Resolves the topology itself — for use outside a Spring context. */
    public DependencyGuard(KafkaClusterProperties properties,
                           ActiveClusterManager clusterManager,
                           BindingLifecycleManager bindingLifecycleManager) {
        this(properties, properties.topology(), clusterManager, bindingLifecycleManager);
    }

    @Autowired
    public DependencyGuard(KafkaClusterProperties properties,
                           ClusterTopology topology,
                           ActiveClusterManager clusterManager,
                           BindingLifecycleManager bindingLifecycleManager) {
        this(properties, topology, clusterManager, bindingLifecycleManager, null, Clock.systemUTC());
    }

    /**
     * @param pauseRequests runs the pauses gates ask for; null for the guard's own daemon thread
     * @param clock         measures how long a record has been held back
     */
    DependencyGuard(KafkaClusterProperties properties,
                    ClusterTopology topology,
                    ActiveClusterManager clusterManager,
                    BindingLifecycleManager bindingLifecycleManager,
                    Executor pauseRequests,
                    Clock clock) {
        this.clock = clock;
        if (pauseRequests == null) {
            this.ownedExecutor = DaemonExecutors.singleThread("kafka-dr-dependency-guard");
            this.pauseRequests = ownedExecutor;
        } else {
            this.ownedExecutor = null;
            this.pauseRequests = pauseRequests;
        }
        this.clusterManager = clusterManager;
        this.bindingLifecycleManager = bindingLifecycleManager;
        for (ConsumerConfig consumer : properties.getConsumers().values()) {
            if (consumer.getDependsOn() == null || consumer.getDependsOn().isEmpty()) {
                continue;
            }
            ClusterTopology.Group group = topology.groupOf(consumer);
            dependents.put(consumer.getName(), new Dependent(
                    consumer.getName(),
                    group == null ? KafkaClusterProperties.DEFAULT_CLUSTER_GROUP : group.name(),
                    consumer.getDependsOn().stream().map(String::trim).toList(),
                    Duration.ofMillis(consumer.getDependsOnNackIntervalMs()),
                    Duration.ofMillis(consumer.getDependsOnMaxHoldMs())));
        }
        if (!dependents.isEmpty()) {
            log.info("Consumer dependencies: {}", dependents.values().stream()
                    .map(d -> d.name() + " -> " + d.dependsOn()).toList());
        }
    }

    @PreDestroy
    void shutdown() {
        if (ownedExecutor != null) {
            ownedExecutor.shutdownNow();
        }
    }

    /** The gate of one consumer; {@link DependencyGate#NONE} when it depends on nothing. */
    public DependencyGate gateFor(String consumerName) {
        Dependent dependent = dependents.get(consumerName);
        return dependent == null ? DependencyGate.NONE : new Gate(dependent);
    }

    // --- events -------------------------------------------------------------------------

    /**
     * A consumer's own group moved: its bindings were just started on the new cluster (the
     * lifecycle manager runs first), and a consumer that is blocked must not start polling there.
     */
    @EventListener
    @Order(BindingLifecycleManager.SWITCH_ORDER + 10)
    public void onClusterSwitched(ClusterSwitchedEvent event) {
        lock.lock();
        try {
            switched(event);
        } finally {
            lock.unlock();
        }
    }

    private void switched(ClusterSwitchedEvent event) {
        startedClusterByGroup.put(event.getGroup(), event.getNewCluster());
        for (Dependent dependent : dependents.values()) {
            if (!dependent.group().equals(event.getGroup())) {
                continue;
            }
            // The binding the consumer was paused on has just been stopped — but spring-kafka keeps
            // the pause request through a stop, so it would start paused when the group comes back
            // to that cluster, with nothing left to resume it. Resuming a stopped binding only
            // clears that request; if the dependency is still down, the new binding is paused below.
            String previouslyPaused = pausedOn.remove(dependent.name());
            if (previouslyPaused != null) {
                bindingLifecycleManager.resumeConsumer(previouslyPaused, dependent.name());
            }
            String blocking = blockingDependency(dependent);
            if (blocking != null) {
                pause(dependent, event.getNewCluster(), blocking);
            }
        }
    }

    /** A dependency went down or came back: pause or resume every consumer that depends on it. */
    @EventListener
    public void onAvailabilityChanged(ClusterGroupAvailabilityEvent event) {
        lock.lock();
        try {
            for (Dependent dependent : dependents.values()) {
                if (dependent.dependsOn().contains(event.getGroup())) {
                    if (!event.isAvailable()) {
                        suspendHolds(dependent);
                    }
                    reconcile(dependent);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Brings one consumer's pause state in line with its dependencies right now. */
    private void reconcile(Dependent dependent) {
        String cluster = startedClusterByGroup.get(dependent.group());
        if (cluster == null) {
            // Its bindings have not been started yet; the switch that starts them decides.
            return;
        }
        String blocking = blockingDependency(dependent);
        boolean paused = pausedOn.containsKey(dependent.name());
        if (blocking != null && !paused) {
            pause(dependent, cluster, blocking);
        } else if (blocking == null && paused) {
            String pausedCluster = pausedOn.remove(dependent.name());
            bindingLifecycleManager.resumeConsumer(pausedCluster, dependent.name());
            log.info("DR_EVENT [{}] Resumed: every dependency {} is available again",
                    dependent.name(), dependent.dependsOn());
        }
    }

    /**
     * Records the pause only if a binding was actually paused. A late-initialized cluster may not
     * have its binding yet; recording the pause anyway would make the gate skip it once the
     * binding exists and starts polling.
     */
    private void pause(Dependent dependent, String cluster, String blocking) {
        if (!bindingLifecycleManager.pauseConsumer(cluster, dependent.name())) {
            log.info("DR_EVENT [{}] Dependency group '{}' has no healthy cluster; no binding on [{}] to pause yet — "
                    + "records reaching it are held back", dependent.name(), blocking, cluster);
            return;
        }
        pausedOn.put(dependent.name(), cluster);
        log.warn("DR_EVENT [{}] Paused: dependency group '{}' has no healthy cluster", dependent.name(), blocking);
    }

    /**
     * Called on a consumer thread: queues the pause for the guard's thread and returns at once.
     */
    private void requestPause(Dependent dependent, String cluster) {
        if (!pauseRequested.add(dependent.name())) {
            return;
        }
        try {
            pauseRequests.execute(() -> {
                pauseRequested.remove(dependent.name());
                ensurePaused(dependent, cluster);
            });
        } catch (RejectedExecutionException e) {
            // Shutting down; the record is held back regardless.
            pauseRequested.remove(dependent.name());
        }
    }

    /**
     * A pause a gate asked for, on the guard's thread. Re-checked under the lock: the dependency
     * may have recovered — and its resume been handled — since the gate looked.
     */
    private void ensurePaused(Dependent dependent, String cluster) {
        lock.lock();
        try {
            if (pausedOn.containsKey(dependent.name())) {
                return;
            }
            String blocking = blockingDependency(dependent);
            if (blocking != null) {
                startedClusterByGroup.putIfAbsent(dependent.group(), cluster);
                pause(dependent, cluster, blocking);
            }
        } finally {
            lock.unlock();
        }
    }

    private String blockingDependency(Dependent dependent) {
        for (String group : dependent.dependsOn()) {
            if (!clusterManager.hasHealthyCluster(group)) {
                return group;
            }
        }
        return null;
    }

    private final class Gate implements DependencyGate {

        private final Dependent dependent;

        private Gate(Dependent dependent) {
            this.dependent = dependent;
        }

        @Override
        public String blockingDependency(String clusterName) {
            String blocking = DependencyGuard.this.blockingDependency(dependent);
            if (blocking != null) {
                suspendHolds(dependent);
                // Covers the paths that start bindings without a switch event — a late-initialized
                // cluster — and the window before the availability event is handled.
                requestPause(dependent, clusterName);
            }
            return blocking;
        }

        @Override
        public boolean isDependencyFailure(String clusterName, Throwable failure) {
            for (Throwable t = failure; t != null; t = t.getCause()) {
                if (t instanceof ClusterGroupUnavailableException unavailable
                        && dependent.dependsOn().contains(unavailable.getGroup())) {
                    return true;
                }
                if (t instanceof SendFailedException) {
                    // The message is proven at fault — unserializable, or refused by the broker for
                    // what it is. Holding it back would only bring it back to fail again, forever;
                    // it takes the ordinary retry path (DLQ, skip) instead.
                    return false;
                }
            }
            return blockingDependency(clusterName) != null;
        }

        @Override
        public Duration nackInterval() {
            return dependent.nackInterval();
        }

        @Override
        public boolean mayHoldBack(String clusterName, Message<?> record) {
            String topic = record.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, String.class);
            Integer partition = record.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION, Integer.class);
            Long offset = record.getHeaders().get(KafkaHeaders.OFFSET, Long.class);
            if (DependencyGuard.this.blockingDependency(dependent) != null) {
                // Waiting for a group that is down is what depends-on is for: not counted.
                suspendHolds(dependent);
                return true;
            }
            if (topic == null || partition == null || offset == null) {
                // Cannot be counted, so it cannot be held within the limit: not held at all.
                log.warn("[{}][{}] Record without topic/partition/offset headers cannot be held back within "
                        + "depends-on-max-hold-ms; it takes the ordinary failure path", clusterName, dependent.name());
                return false;
            }
            String key = topic + "-" + partition;
            Instant now = clock.instant();
            boolean[] expiredNow = new boolean[1];
            Hold hold = holdsOf(dependent).compute(key, (k, held) -> {
                Hold h = held != null && held.offset == offset ? held : new Hold(offset);
                if (h.expired) {
                    return h;
                }
                if (h.lastSeen != null) {
                    h.heldWhileUp = h.heldWhileUp.plus(Duration.between(h.lastSeen, now));
                }
                h.lastSeen = now;
                if (h.heldWhileUp.compareTo(dependent.maxHold()) >= 0) {
                    h.expired = true;
                    expiredNow[0] = true;
                }
                return h;
            });
            if (expiredNow[0]) {
                log.error("[{}][{}] Record {} offset {} held back for {} s with {} available, past "
                                + "depends-on-max-hold-ms={}: it now takes the ordinary failure path (retries, "
                                + "then DLQ or skip) and is not held again",
                        clusterName, dependent.name(), key, offset, hold.heldWhileUp.toSeconds(),
                        dependent.dependsOn(), dependent.maxHold().toMillis());
            }
            // Stays expired for every redelivery of the same record — the binder's retries, the
            // container's error handler — until the partition moves on to another record.
            return !hold.expired;
        }
    }

    private Map<String, Hold> holdsOf(Dependent dependent) {
        return holds.computeIfAbsent(dependent.name(), name -> new ConcurrentHashMap<>());
    }

    /**
     * A dependency is down: the time until a held record is next seen must not count. The time
     * held so far stays — a record cannot restart its own count by taking the group down.
     */
    private void suspendHolds(Dependent dependent) {
        Map<String, Hold> held = holdsOf(dependent);
        // Per key under the map's lock — replaceAll would run outside it and could lose the reset
        // to a consumer thread inside compute for the same partition. Not covered by a test: the
        // window cannot be hit deterministically, and a lost reset would heal on the next call —
        // every check while the dependency is down suspends again.
        for (String key : held.keySet()) {
            held.computeIfPresent(key, (k, hold) -> {
                hold.lastSeen = null;
                return hold;
            });
        }
    }

    /** One held record of a partition; mutated only inside the map's compute for its key. */
    private static final class Hold {
        private final long offset;
        private Duration heldWhileUp = Duration.ZERO;
        /** When the hold was last counted; null while a dependency is down. */
        private Instant lastSeen;
        private boolean expired;

        private Hold(long offset) {
            this.offset = offset;
        }
    }

}
