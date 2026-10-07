package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.concurrent.DaemonExecutors;
import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.StartupClusterState;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Dynamically creates consumer bindings for clusters that were unreachable at startup.
 * When a cluster becomes reachable, this component:
 * 1. Creates the binder (child context) via BinderFactory
 * 2. Binds consumer channels to topics
 * 3. Wires function beans to the channels
 * 4. Marks the cluster as initialized in StartupClusterState
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class LateBindingInitializer {

    private static final Logger log = LoggerFactory.getLogger(LateBindingInitializer.class);

    private final KafkaClusterProperties properties;
    private final ClusterTopology topology;
    private final StartupClusterState startupState;
    private final ActiveClusterManager clusterManager;
    private final BinderFactory binderFactory;
    private final BindingServiceProperties bindingServiceProperties;
    private final ApplicationContext applicationContext;
    private final Map<String, Binding<?>> lateBindings = new ConcurrentHashMap<>();
    /** Probes the uninitialized clusters in parallel, so one group's dead clusters never delay another's. */
    private final Executor probes;
    /** The probe pool when this bean created it, shut down with it. */
    private final ExecutorService ownedExecutor;

    /** Resolves the topology itself — for use outside a Spring context. */
    public LateBindingInitializer(KafkaClusterProperties properties,
                                  StartupClusterState startupState,
                                  ActiveClusterManager clusterManager,
                                  BinderFactory binderFactory,
                                  BindingServiceProperties bindingServiceProperties,
                                  ApplicationContext applicationContext) {
        this(properties, properties.topology(), startupState, clusterManager, binderFactory,
                bindingServiceProperties, applicationContext);
    }

    /**
     * The consumer function beans are deliberately not looked up here. A consumer with
     * {@code depends-on} needs the {@code DependencyGuard} to be built, the guard needs the
     * binding lifecycle manager, and the manager needs this bean — looking the function beans up
     * now would close that cycle and silently lose those consumers for every cluster initialized
     * late. They are resolved when a cluster is initialized, long after the context is up.
     */
    @Autowired
    public LateBindingInitializer(KafkaClusterProperties properties,
                                  ClusterTopology topology,
                                  StartupClusterState startupState,
                                  ActiveClusterManager clusterManager,
                                  BinderFactory binderFactory,
                                  BindingServiceProperties bindingServiceProperties,
                                  ApplicationContext applicationContext) {
        this(properties, topology, startupState, clusterManager, binderFactory, bindingServiceProperties,
                applicationContext, null);
    }

    /**
     * @param probes runs the reachability probes; null for a pool with a thread per cluster
     */
    LateBindingInitializer(KafkaClusterProperties properties,
                           ClusterTopology topology,
                           StartupClusterState startupState,
                           ActiveClusterManager clusterManager,
                           BinderFactory binderFactory,
                           BindingServiceProperties bindingServiceProperties,
                           ApplicationContext applicationContext,
                           Executor probes) {
        if (probes == null) {
            this.ownedExecutor = DaemonExecutors.fixedPool("kafka-dr-late-probe-", topology.clusters().size());
            this.probes = ownedExecutor;
        } else {
            this.ownedExecutor = null;
            this.probes = probes;
        }
        this.properties = properties;
        this.topology = topology;
        this.startupState = startupState;
        this.clusterManager = clusterManager;
        this.binderFactory = binderFactory;
        this.bindingServiceProperties = bindingServiceProperties;
        this.applicationContext = applicationContext;
    }

    private static boolean reachable(String cluster, CompletableFuture<Boolean> probe) {
        try {
            return Boolean.TRUE.equals(probe.join());
        } catch (CompletionException | CancellationException e) {
            log.debug("[{}] Late-initialization probe failed: {}", cluster, e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<Message<?>> functionBean(String cluster, String consumerName, String beanName) {
        try {
            return applicationContext.getBean(beanName, Consumer.class);
        } catch (Exception e) {
            // A missing function bean means a consumer that never consumes on this cluster:
            // worth a warning, not a debug line.
            log.warn("[{}][{}] Function bean '{}' not available, consumer skipped on this cluster: {}",
                    cluster, consumerName, beanName, e.getMessage());
            return null;
        }
    }

    @PreDestroy
    void shutdown() {
        if (ownedExecutor != null) {
            ownedExecutor.shutdownNow();
        }
    }

    /**
     * Probes every uninitialized cluster at once, then initializes those that answered. Probing
     * one after another would make a group whose clusters are black-holed — each probe waiting out
     * its timeout — delay a cluster of another group that is already reachable.
     */
    @Scheduled(fixedDelayString = "${kafka-dr.late-initializer.timeout-ms:5000}")
    public void checkAndInitializeClusters() {
        Map<ClusterTopology.ClusterRef, CompletableFuture<Boolean>> reachability = new LinkedHashMap<>();
        for (ClusterTopology.ClusterRef ref : topology.clusters()) {
            if (!startupState.isInitialized(ref.id())) {
                reachability.put(ref, CompletableFuture.supplyAsync(
                        () -> KafkaAdminHelper.probeCluster(ref.id(), properties), probes));
            }
        }

        for (Map.Entry<ClusterTopology.ClusterRef, CompletableFuture<Boolean>> probe : reachability.entrySet()) {
            ClusterTopology.ClusterRef ref = probe.getKey();
            String cluster = ref.id();
            if (!reachable(cluster, probe.getValue())) {
                continue;
            }

            try {
                if (topology.groupOfCluster(cluster).autoCreateTopics()) {
                    KafkaAdminHelper.provisionTopics(cluster, ref.bootstrapServers(), properties, topology);
                }
                initializeCluster(ref);
                startupState.addInitializedCluster(cluster);
                log.info("DR_EVENT [{}] Late-initialized — bindings created", cluster);

                // If this cluster is already active (switch happened before bindings existed), start consumers now
                if (cluster.equals(clusterManager.getActiveCluster(ref.group()))) {
                    startBindings(cluster);
                    log.info("[{}] Started late bindings (already-active)", cluster);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to late-initialize: {}", cluster, e.getMessage(), e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initializeCluster(ClusterTopology.ClusterRef ref) {
        String cluster = ref.id();
        log.info("[{}] Late-initializing...", cluster);

        Binder<MessageChannel, ? extends ConsumerProperties, ?> binder =
                (Binder<MessageChannel, ? extends ConsumerProperties, ?>)
                        binderFactory.getBinder(cluster, MessageChannel.class);

        // Only the consumers of this cluster's group: the others read from another Kafka.
        for (KafkaClusterProperties.ConsumerConfig consumer : topology.groupOfCluster(cluster).consumers()) {
            String consumerName = consumer.getName();
            String topic = consumer.getTopic();
            String functionName = ref.functionName(consumerName);
            String bindingName = ref.bindingName(consumerName);

            Consumer<Message<?>> handler = functionBean(cluster, consumerName, functionName);
            if (handler == null) {
                continue;
            }

            // Create input channel and wire to handler.
            // Exceptions are deliberately NOT swallowed here: the startup-bound path lets
            // them reach the container's error handler, and a late-bound cluster must
            // behave identically or the two diverge after a failover.
            DirectChannel channel = new DirectChannel();
            channel.subscribe(handler::accept);

            // Get binding properties from environment
            var bindingProps = bindingServiceProperties.getBindingProperties(bindingName);
            String group = bindingProps.getGroup();
            String destination = bindingProps.getDestination();
            if (destination == null) destination = topic;
            if (group == null) group = consumer.getGroup();

            // Take both property namespaces as Spring Cloud Stream already resolved them
            // for this binding, so a late-initialized cluster gets exactly the configuration
            // a startup-initialized one got — including spring.cloud.stream.default.* .
            // Copying selected keys by hand is what let ack-mode, concurrency, batch-mode
            // and DLQ settings diverge between clusters.
            var kafkaConsumerProps = extendedConsumerProperties(binder, bindingName);
            var extendedProps = new ExtendedConsumerProperties<>(kafkaConsumerProps);
            BeanUtils.copyProperties(
                    bindingServiceProperties.getConsumerProperties(bindingName),
                    extendedProps,
                    ConsumerProperties.class);

            // Lifecycle stays with the starter regardless of what was configured.
            extendedProps.setAutoStartup(false);
            // copyProperties cannot carry it (there is no setter), and the container customizer
            // matches a container to its consumer — and cluster group — by binding name.
            extendedProps.populateBindingName(bindingName);

            // Bind
            var binding = ((Binder<MessageChannel, ExtendedConsumerProperties<KafkaConsumerProperties>, ?>) binder)
                    .bindConsumer(destination, group, channel, extendedProps);

            lateBindings.put(bindingName, binding);
            log.info("[{}][{}] Created late binding: {}", cluster, consumerName, bindingName);
        }
    }

    /**
     * Kafka-specific consumer properties for a binding, as resolved by the binder itself.
     * Every Kafka binder implements {@link ExtendedPropertiesBinder}; the fallback exists
     * only so a stubbed or non-extended binder cannot break late initialization.
     */
    private static KafkaConsumerProperties extendedConsumerProperties(Binder<?, ?, ?> binder, String bindingName) {
        if (binder instanceof ExtendedPropertiesBinder<?, ?, ?> extended) {
            Object props = extended.getExtendedConsumerProperties(bindingName);
            if (props instanceof KafkaConsumerProperties kafkaProps) {
                return kafkaProps;
            }
        }
        log.warn("Binder {} does not expose extended consumer properties for '{}' — "
                        + "Kafka-specific settings (ack-mode, DLQ, client configuration) will not be applied",
                binder.getClass().getSimpleName(), bindingName);
        return new KafkaConsumerProperties();
    }

    /**
     * Start consumer bindings for a late-initialized cluster.
     * Called by BindingLifecycleManager when switching to this cluster.
     */
    public void startBindings(String cluster) {
        ClusterTopology.ClusterRef ref = topology.findCluster(cluster);
        if (ref == null) {
            return;
        }
        for (KafkaClusterProperties.ConsumerConfig consumer : topology.groupOfCluster(cluster).consumers()) {
            String bindingName = ref.bindingName(consumer.getName());
            var binding = lateBindings.get(bindingName);
            if (binding != null) {
                binding.start();
                log.info("[{}][{}] Started late binding", cluster, consumer.getName());
            }
        }
    }

    /**
     * Pauses one late binding.
     *
     * @return false when the binding does not exist yet, so there was nothing to pause
     */
    public boolean pauseBinding(String bindingName) {
        Binding<?> binding = lateBindings.get(bindingName);
        if (binding == null) {
            return false;
        }
        binding.pause();
        return true;
    }

    /**
     * Resumes a late binding paused by {@link #pauseBinding}.
     *
     * @return false when the binding does not exist yet
     */
    public boolean resumeBinding(String bindingName) {
        Binding<?> binding = lateBindings.get(bindingName);
        if (binding == null) {
            return false;
        }
        binding.resume();
        return true;
    }

    /**
     * Stop consumer bindings for a late-initialized cluster.
     */
    public void stopBindings(String cluster) {
        ClusterTopology.ClusterRef ref = topology.findCluster(cluster);
        if (ref == null) {
            return;
        }
        for (KafkaClusterProperties.ConsumerConfig consumer : topology.groupOfCluster(cluster).consumers()) {
            String bindingName = ref.bindingName(consumer.getName());
            var binding = lateBindings.get(bindingName);
            if (binding != null) {
                binding.stop();
                log.info("[{}][{}] Stopped late binding", cluster, consumer.getName());
            }
        }
    }

}
