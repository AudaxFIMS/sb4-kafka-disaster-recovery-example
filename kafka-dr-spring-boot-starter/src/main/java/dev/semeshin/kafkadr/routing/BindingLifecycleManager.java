package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.StartupClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Moves the consumer (input) bindings of a cluster group to its new active cluster on a
 * switch. Supports both startup-initialized and late-initialized clusters.
 *
 * <p>Producer bindings need nothing here: {@code ResilientProducer} names the target
 * cluster on every send, and StreamBridge caches one output channel per binder and
 * binding, so a switch simply starts using another cache entry.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class BindingLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(BindingLifecycleManager.class);

    /** Listener order of {@link #onClusterSwitched}; later listeners see the bindings moved. */
    public static final int SWITCH_ORDER = 0;

    private final BindingsLifecycleController bindingsController;
    private final StartupClusterState startupState;
    private final LateBindingInitializer lateBindingInitializer;
    private final ClusterTopology topology;
    private final Map<String, List<String>> inputBindingsByCluster;
    /** Cluster id of every input binding — the reverse of {@link #inputBindingsByCluster}. */
    private final Map<String, String> clusterByInputBinding = new HashMap<>();
    /** Whose election decides whether a binding created late should run; absent outside Spring. */
    private final ObjectProvider<ActiveClusterManager> clusterManager;
    private final Set<String> startupClusters;
    /** Diagnostic logging switch — see {@code kafka-dr.debug.enable}. */
    private final boolean debugEnabled;

    /** Resolves the topology itself — for use outside a Spring context. */
    public BindingLifecycleManager(BindingsLifecycleController bindingsController,
                                   KafkaClusterProperties properties,
                                   StartupClusterState startupState,
                                   LateBindingInitializer lateBindingInitializer) {
        this(bindingsController, properties, properties.topology(), startupState, lateBindingInitializer);
    }

    public BindingLifecycleManager(BindingsLifecycleController bindingsController,
                                   KafkaClusterProperties properties,
                                   ClusterTopology topology,
                                   StartupClusterState startupState,
                                   LateBindingInitializer lateBindingInitializer) {
        this(bindingsController, properties, topology, startupState, lateBindingInitializer, null);
    }

    @Autowired
    public BindingLifecycleManager(BindingsLifecycleController bindingsController,
                                   KafkaClusterProperties properties,
                                   ClusterTopology topology,
                                   StartupClusterState startupState,
                                   LateBindingInitializer lateBindingInitializer,
                                   ObjectProvider<ActiveClusterManager> clusterManager) {
        this.clusterManager = clusterManager;
        this.bindingsController = bindingsController;
        this.startupState = startupState;
        this.lateBindingInitializer = lateBindingInitializer;
        this.debugEnabled = properties.getDebug().isEnable();
        this.startupClusters = Set.copyOf(startupState.getInitializedClusters());
        this.topology = topology;
        this.inputBindingsByCluster = buildInputBindingIndex(topology);
        inputBindingsByCluster.forEach((cluster, bindings) -> bindings.forEach(b -> clusterByInputBinding.put(b, cluster)));
        log.info("Input bindings by cluster: {}", inputBindingsByCluster);
    }

    /**
     * Every input binding a binder creates. Normally the controller knows each by name; one that
     * Spring Cloud Stream created after startup — its topic was missing then, and the binding
     * service retried until it appeared — it does not: the binding service keeps a placeholder
     * under the topic's name that passes on nothing but {@code unbind}. For such a binding this
     * is the only handle to start, stop, pause or resume it.
     */
    private final Map<String, Binding<?>> createdBindings = new ConcurrentHashMap<>();

    /**
     * Records the binding, and starts it when it arrives late on the active cluster of an elected
     * group: like every DR binding it is created stopped, and the switch that would have started
     * it happened long ago — it would stay idle until a restart.
     */
    @EventListener
    public synchronized void onBindingCreated(BindingCreatedEvent event) {
        if (!(event.getSource() instanceof Binding<?> binding) || !binding.isInput()) {
            return;
        }
        String cluster = clusterByInputBinding.get(binding.getBindingName());
        // Late-initialized clusters start their own bindings, see LateBindingInitializer.
        if (cluster == null || !startupClusters.contains(cluster)) {
            return;
        }
        createdBindings.put(binding.getBindingName(), binding);
        ActiveClusterManager manager = clusterManager == null ? null : clusterManager.getIfAvailable();
        if (manager == null) {
            return;
        }
        String group = topology.groupOfCluster(cluster).name();
        // Before the initial election nothing runs yet; the election's switch starts it. On a
        // standby, the switch that makes it active will — through the record above.
        if (!manager.hasHealthyCluster(group) || !cluster.equals(manager.getActiveCluster(group))
                || bindingsController.queryState(binding.getBindingName()).size() > 0) {
            return;
        }
        try {
            binding.start();
            log.info("[{}] Started binding created after startup: {}", cluster, binding.getBindingName());
        } catch (Exception e) {
            if (debugEnabled) {
                log.error("[{}] Failed to start late-created binding '{}'", cluster, binding.getBindingName(), e);
            } else {
                log.error("[{}] Failed to start late-created binding '{}': {}", cluster, binding.getBindingName(),
                        e.getMessage());
            }
        }
    }

    /**
     * Changes one binding of a startup-initialized cluster: through the controller, or — for a
     * binding created after startup, which the controller cannot find — through the binding
     * itself.
     */
    private void changeState(String binding, State state) {
        Binding<?> created = createdBindings.get(binding);
        if (created == null || !bindingsController.queryState(binding).isEmpty()) {
            bindingsController.changeState(binding, state);
            return;
        }
        switch (state) {
            case STARTED -> created.start();
            case STOPPED -> created.stop();
            case PAUSED -> created.pause();
            case RESUMED -> created.resume();
            default -> bindingsController.changeState(binding, state);
        }
    }

    /** Whether the binding exists — known to the controller, or created after startup. */
    private boolean exists(String binding) {
        return !bindingsController.queryState(binding).isEmpty() || createdBindings.containsKey(binding);
    }

    /**
     * Runs before every other listener of the switch, so that anything reacting to it — the
     * dependency guard pausing a consumer, for one — finds the new cluster's bindings started.
     */
    @EventListener
    @Order(SWITCH_ORDER)
    public synchronized void onClusterSwitched(ClusterSwitchedEvent event) {
        String previous = event.getPreviousCluster();
        String next = event.getNewCluster();

        log.info("DR_EVENT [{}] -> [{}] Switching bindings", previous, next);

        stopInputBindings(previous);
        startInputBindings(next);
    }

    private void stopInputBindings(String cluster) {
        if (startupClusters.contains(cluster)) {
            // Startup-initialized: use BindingsLifecycleController
            for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
                try {
                    changeState(binding, State.STOPPED);
                    log.info("[{}] Stopped binding: {}", cluster, binding);
                } catch (Exception e) {
                    if (debugEnabled) {
                        log.error("[{}] Failed to stop binding '{}'", cluster, binding, e);
                    } else {
                        log.error("[{}] Failed to stop binding '{}': {}", cluster, binding, e.getMessage());
                    }
                }
            }
        } else if (startupState.isInitialized(cluster)) {
            // Late-initialized: use LateBindingInitializer
            lateBindingInitializer.stopBindings(cluster);
        }
    }

    private void startInputBindings(String cluster) {
        if (!startupState.isInitialized(cluster)) {
            log.warn("[{}] Not yet initialized, cannot start bindings", cluster);
            return;
        }

        if (startupClusters.contains(cluster)) {
            // Startup-initialized: use BindingsLifecycleController
            for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
                try {
                    changeState(binding, State.STARTED);
                    log.info("[{}] Started binding: {}", cluster, binding);
                } catch (Exception e) {
                    if (debugEnabled) {
                        log.error("[{}] Failed to start binding '{}'", cluster, binding, e);
                    } else {
                        log.error("[{}] Failed to start binding '{}': {}", cluster, binding, e.getMessage());
                    }
                }
            }
        } else {
            // Late-initialized: use LateBindingInitializer
            lateBindingInitializer.startBindings(cluster);
        }
    }

    /**
     * Pauses one consumer's binding on a cluster: the container stops polling but keeps its
     * partitions, so there is no rebalance and nothing to seek when it resumes.
     *
     * <p>A paused container stays paused through a stop and a later start — spring-kafka keeps
     * the request — so whoever pauses a binding must also resume it, even after it was stopped.
     *
     * @return whether a binding was actually paused; false for a late-initialized cluster whose
     *         binding does not exist yet, or when the change failed
     */
    public boolean pauseConsumer(String cluster, String consumerName) {
        return changeConsumerState(cluster, consumerName, State.PAUSED);
    }

    /**
     * Resumes a binding paused by {@link #pauseConsumer} — also a stopped one, which clears the
     * pause it would otherwise start with next time.
     */
    public boolean resumeConsumer(String cluster, String consumerName) {
        return changeConsumerState(cluster, consumerName, State.RESUMED);
    }

    private synchronized boolean changeConsumerState(String cluster, String consumerName, State state) {
        ClusterTopology.ClusterRef ref = topology.findCluster(cluster);
        if (ref == null) {
            log.warn("[{}] Unknown cluster, cannot {} consumer '{}'", cluster, state, consumerName);
            return false;
        }
        String binding = ref.bindingName(consumerName);
        try {
            boolean changed;
            if (startupClusters.contains(cluster)) {
                // changeState silently does nothing for a name it does not know, so a pause it
                // "made" on a missing binding would be recorded and never retried.
                changed = exists(binding);
                if (changed) {
                    changeState(binding, state);
                }
            } else if (state == State.PAUSED) {
                changed = lateBindingInitializer.pauseBinding(binding);
            } else {
                changed = lateBindingInitializer.resumeBinding(binding);
            }
            if (changed) {
                log.info("[{}] {} binding: {}", cluster, state == State.PAUSED ? "Paused" : "Resumed", binding);
            } else {
                log.debug("[{}] Binding '{}' does not exist yet, nothing to {}", cluster, binding, state);
            }
            return changed;
        } catch (Exception e) {
            if (debugEnabled) {
                log.error("[{}] Failed to {} binding '{}'", cluster, state, binding, e);
            } else {
                log.error("[{}] Failed to {} binding '{}': {}", cluster, state, binding, e.getMessage());
            }
            return false;
        }
    }

    /** Input bindings per cluster id — only the consumers of that cluster's own group. */
    private static Map<String, List<String>> buildInputBindingIndex(ClusterTopology topology) {
        Map<String, List<String>> index = new HashMap<>();
        for (ClusterTopology.Group group : topology.groups()) {
            for (ClusterTopology.ClusterRef ref : group.clusters()) {
                List<String> bindings = new ArrayList<>();
                for (KafkaClusterProperties.ConsumerConfig consumer : group.consumers()) {
                    bindings.add(ref.bindingName(consumer.getName()));
                }
                index.put(ref.id(), bindings);
            }
        }
        return index;
    }
}
