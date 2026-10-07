package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.consumer.*;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.routing.DependencyGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Consumer;

/**
 * Generates Spring Cloud Stream binders, bindings, and consumer function beans
 * at startup based on kafka-dr configuration. Runs as BeanDefinitionRegistryPostProcessor
 * to ensure properties and beans are registered before Spring Cloud Function initializes.
 */
@Component
public class DynamicBindingRegistrar implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DynamicBindingRegistrar.class);

    /** StreamBridge's cache of producer channels, one entry per (cluster, producer) it has used. */
    private static final String DYNAMIC_DESTINATION_CACHE_SIZE = "spring.cloud.stream.dynamic-destination-cache-size";
    /** Spring Cloud Stream's own default for that cache. */
    private static final int DEFAULT_DYNAMIC_DESTINATION_CACHE_SIZE = 10;

    /** Binder-side lazy topic creation; the starter provisions topics itself instead. */
    private static final String BINDER_AUTO_CREATE_TOPICS =
            "spring.cloud.stream.kafka.binder.auto-create-topics";

    private ConfigurableEnvironment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = (ConfigurableEnvironment) environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        Boolean enabled = environment.getProperty("kafka-dr.enabled", Boolean.class, false);
        if (!enabled) {
            return;
        }

        KafkaClusterProperties props = Binder.get(environment)
                .bind("kafka-dr", KafkaClusterProperties.class)
                .orElse(null);

        if (props == null) {
            log.warn("kafka-dr.enabled=true but no clusters configured, skipping");
            return;
        }
        ClusterTopology topology = ClusterTopologyValidator.validate(props);
        if (topology.isEmpty()) {
            log.warn("kafka-dr.enabled=true but no clusters configured, skipping");
            return;
        }
        if (topology.isMultiGroup()) {
            log.info("Cluster groups: {}", topology.groups().stream()
                    .map(g -> g.name() + g.clusters().stream().map(ClusterTopology.ClusterRef::id).toList())
                    .toList());
        }

        // Remove Spring Boot's default KafkaAdmin to prevent it from connecting
        // to localhost:9092 and blocking startup. DR manages its own AdminClients.
        if (registry.containsBeanDefinition("kafkaAdmin")) {
            registry.removeBeanDefinition("kafkaAdmin");
        }

        // Static utility, so the flag has to be pushed in — and before the first probe.
        KafkaAdminHelper.setDebugEnabled(props.getDebug().isEnable());

        ConsumerConfigValidator.validate(props, topology);

        Set<String> reachableClusters = probeAllClusters(topology, props);
        log.info("Reachable clusters at startup: {}", reachableClusters);

        Map<String, Object> generated = new LinkedHashMap<>();
        List<String> functionNames = new ArrayList<>();

        // Generate binder configs for ALL clusters (just environment properties)
        generateBinders(topology, props, generated);
        // Generate consumer binding properties for ALL clusters
        generateConsumerBindingProperties(topology, props, generated);
        // But only include reachable clusters in function definition
        // (unreachable clusters get bindings created later by LateBindingInitializer)
        generateFunctionDefinitions(topology, functionNames, reachableClusters);
        generateProducerBindings(props, generated);
        sizeProducerChannelCache(topology, props, generated);

        if (!functionNames.isEmpty()) {
            generated.put("spring.cloud.function.definition", String.join(";", functionNames));
        }

        environment.getPropertySources().addFirst(
                new MapPropertySource("kafka-dr-dynamic-bindings", generated));

        log.info("Generated bindings for {} reachable clusters, properties for all {} clusters",
                reachableClusters.size(), topology.clusters().size());

        for (ClusterTopology.ClusterRef ref : topology.clusters()) {
            if (reachableClusters.contains(ref.id()) && topology.groupOfCluster(ref.id()).autoCreateTopics()) {
                KafkaAdminHelper.provisionTopics(ref.id(), ref.bootstrapServers(), props, topology);
            }
        }

        // Register function beans for ALL clusters (needed for late binding)
        registerConsumerBeans(registry, topology, props);

        // Store reachable clusters in environment so StartupClusterState can read them
        generated.put("kafka-dr.internal.initialized-clusters", String.join(",", reachableClusters));
    }

    private static void putIfNotNull(Map<String, Object> generated, String key, Object value) {
        if (value != null) {
            generated.put(key, value.toString());
        }
    }

    private Set<String> probeAllClusters(ClusterTopology topology, KafkaClusterProperties props) {
        Set<String> reachable = new LinkedHashSet<>();
        for (ClusterTopology.ClusterRef ref : topology.clusters()) {
            if (KafkaAdminHelper.probeCluster(ref.id(), props)) {
                reachable.add(ref.id());
            } else {
                log.warn("[{}] Unreachable at startup — will be initialized later", ref.id());
            }
        }
        return reachable;
    }

    private void generateBinders(ClusterTopology topology, KafkaClusterProperties props,
                                 Map<String, Object> generated) {
        for (ClusterTopology.ClusterRef ref : topology.clusters()) {
            String clusterName = ref.id();
            String binderPrefix = "spring.cloud.stream.binders." + clusterName;
            String envPrefix = binderPrefix + ".environment";

            generated.put(binderPrefix + ".type", "kafka");
            generated.put(envPrefix + ".spring.cloud.stream.kafka.binder.brokers", ref.bootstrapServers());

            // Lazy topic creation by the binder is off by default, because the starter
            // provisions topics itself: kafka-dr.auto-create-topics reaches every cluster,
            // including the standby ones that have no bindings and would otherwise get
            // their topics only at failover. Lazy creation also drags in
            // KafkaTopicProvisioner, which blocks on metadata lookups against dead brokers
            // (max.block.ms) instead of failing cleanly on send.
            //
            // Written before the configured environment so an explicit
            // default-environment / per-cluster value still wins.
            generated.put(envPrefix + "." + BINDER_AUTO_CREATE_TOPICS, "false");

            for (Map.Entry<String, String> envEntry : props.getEffectiveEnvironment(clusterName).entrySet()) {
                generated.put(envPrefix + "." + envEntry.getKey(), envEntry.getValue());
            }
        }
    }

    /**
     * Generates binding PROPERTIES for all clusters (destination, group, binder, etc.).
     * These are just properties in the environment — they don't trigger binder creation.
     * Binder child contexts are only created when a function references the binding.
     */
    private void generateConsumerBindingProperties(ClusterTopology topology, KafkaClusterProperties props,
                                                   Map<String, Object> generated) {
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers().values()) {
            String consumerName = consumer.getName();
            String topic = consumer.getTopic();

            // Only the clusters of the consumer's own group: the topic lives in that Kafka.
            for (ClusterTopology.ClusterRef ref : topology.groupOf(consumer).clusters()) {
                String bindingName = ref.bindingName(consumerName);
                String prefix = "spring.cloud.stream.bindings." + bindingName;

                generated.put(prefix + ".destination", topic);
                generated.put(prefix + ".group", consumer.getGroup());
                generated.put(prefix + ".binder", ref.id());
                generated.put(prefix + ".consumer.auto-startup", "false");

                if ("native".equalsIgnoreCase(consumer.getContentType())) {
                    generated.put(prefix + ".consumer.use-native-decoding", "true");
                }

                String corePrefix = prefix + ".consumer";
                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + bindingName + ".consumer";

                // Batch settings are written before the user's properties so that an
                // explicit configuration.max.poll.records still wins.
                KafkaClusterProperties.BatchConfig batch = consumer.getBatch();
                if (batch.isEnabled()) {
                    generated.put(corePrefix + ".batch-mode", "true");
                    putIfNotNull(generated, kafkaPrefix + ".configuration.max.poll.records", batch.getMaxRecords());
                    putIfNotNull(generated, kafkaPrefix + ".configuration.fetch.min.bytes", batch.getMinBytes());
                    putIfNotNull(generated, kafkaPrefix + ".configuration.fetch.max.wait.ms", batch.getMaxWaitMs());
                }

                for (Map.Entry<String, String> prop : props.getEffectiveConsumerProperties(consumer).entrySet()) {
                    BindingPropertyRouter.checkNotReserved(prop.getKey(), consumerName, false);
                    String target = BindingPropertyRouter.forConsumerKey(prop.getKey(), consumerName)
                            == BindingPropertyRouter.Namespace.CORE ? corePrefix : kafkaPrefix;
                    generated.put(target + "." + prop.getKey(), prop.getValue());
                }
            }
        }
    }

    /**
     * Only include reachable clusters in function definition.
     * Functions for unreachable clusters have beans registered but aren't in the definition,
     * so Spring Cloud Stream doesn't create their bindings (and doesn't create the binder child context).
     */
    private void generateFunctionDefinitions(ClusterTopology topology,
                                             List<String> functionNames,
                                             Set<String> reachableClusters) {
        for (ClusterTopology.Group group : topology.groups()) {
            for (KafkaClusterProperties.ConsumerConfig consumer : group.consumers()) {
                for (ClusterTopology.ClusterRef ref : group.clusters()) {
                    if (reachableClusters.contains(ref.id())) {
                        functionNames.add(ref.functionName(consumer.getName()));
                    }
                }
            }
        }
    }

    private void generateProducerBindings(KafkaClusterProperties props, Map<String, Object> generated) {
        for (KafkaClusterProperties.ProducerConfig producer : props.getProducers().values()) {
            String producerName = producer.getName();
            String topic = producer.getTopic();
            String bindingName = KafkaClusterProperties.producerBindingName(producerName);
            Map<String, String> effective = props.getEffectiveProducerProperties(producer);
            String sync = effective.get("sync");
            if (sync != null && !Boolean.parseBoolean(sync.trim())) {
                throw new IllegalStateException(
                        ("Producer '%s' sets sync=%s. ResilientProducer needs synchronous sends: asynchronously, "
                                + "StreamBridge reports success before the broker answers, so no failure would "
                                + "ever fail over, be reported, or hold a depends-on record back. Remove the "
                                + "setting; the starter sets sync=true itself.").formatted(producerName, sync));
            }

            for (String outBinding : List.of(bindingName, bindingName + "-out-0")) {
                String prefix = "spring.cloud.stream.bindings." + outBinding;
                generated.put(prefix + ".destination", topic);

                if ("native".equalsIgnoreCase(producer.getContentType())) {
                    generated.put(prefix + ".producer.use-native-encoding", "true");
                }

                String corePrefix = prefix + ".producer";
                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + outBinding + ".producer";

                for (Map.Entry<String, String> prop : effective.entrySet()) {
                    BindingPropertyRouter.checkNotReserved(prop.getKey(), producerName, true);
                    String target = BindingPropertyRouter.forProducerKey(prop.getKey(), producerName)
                            == BindingPropertyRouter.Namespace.CORE ? corePrefix : kafkaPrefix;
                    generated.put(target + "." + prop.getKey(), prop.getValue());
                }
                // Every failure the producer acts on surfaces only from a synchronous send.
                generated.put(kafkaPrefix + ".sync", "true");
            }
        }
    }

    /**
     * StreamBridge caches one output channel per (cluster, producer) it has sent through and, once
     * the cache is full, unbinds the eldest entry — possibly a producer still in use, rebuilt on
     * its next send. After a failover and a failback every producer has used two clusters, so the
     * default of 10 is outgrown by six producers already. Sized to every producer on every cluster
     * of its group unless set explicitly.
     */
    private void sizeProducerChannelCache(ClusterTopology topology, KafkaClusterProperties props,
                                          Map<String, Object> generated) {
        int needed = 0;
        for (KafkaClusterProperties.ProducerConfig producer : props.getProducers().values()) {
            ClusterTopology.Group group = topology.groupOf(producer);
            needed += group == null ? 1 : group.clusters().size();
        }
        Integer configured = environment.getProperty(DYNAMIC_DESTINATION_CACHE_SIZE, Integer.class);
        if (configured == null) {
            generated.put(DYNAMIC_DESTINATION_CACHE_SIZE,
                    String.valueOf(Math.max(DEFAULT_DYNAMIC_DESTINATION_CACHE_SIZE, needed)));
        } else if (configured < needed) {
            log.warn("{}={} is below the {} producer channels a failover can open (every producer on every cluster "
                            + "of its group); StreamBridge will unbind producers still in use and rebuild them",
                    DYNAMIC_DESTINATION_CACHE_SIZE, configured, needed);
        }
    }

    private void registerConsumerBeans(BeanDefinitionRegistry registry, ClusterTopology topology,
                                       KafkaClusterProperties props) {
        if (props.getConsumers().isEmpty()) return;

        BeanFactory beanFactory = (BeanFactory) registry;

        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers().values()) {
            String consumerName = consumer.getName();
            for (ClusterTopology.ClusterRef ref : topology.groupOf(consumer).clusters()) {
                String beanName = ref.functionName(consumerName);
                String cluster = ref.id();

                GenericBeanDefinition beanDef = new GenericBeanDefinition();
                beanDef.setBeanClass(Consumer.class);
                beanDef.setInstanceSupplier(() -> {
                    // Master switch: with idempotency disabled the store bean is never
                    // looked up, so even a user-defined store (e.g. Redis) is ignored.
                    IdempotencyStore store = props.isIdempotencyEnabled(consumer)
                            ? beanFactory.getBean(IdempotencyStore.class)
                            : IdempotencyStore.DISABLED;
                    MessageHandlerRegistry handlerRegistry = beanFactory.getBean(MessageHandlerRegistry.class);
                    // The consumer's own watermarks: another consumer of the same topic progresses
                    // independently, and another group is another Kafka altogether.
                    LastProcessedTimestampTracker tracker = beanFactory.getBean(LastProcessedTimestampTracker.class)
                            .forConsumer(consumerName);
                    // Looked up only when needed, so a consumer without depends-on never touches it.
                    DependencyGate gate = consumer.getDependsOn().isEmpty()
                            ? DependencyGate.NONE
                            : beanFactory.getBean(DependencyGuard.class).gateFor(consumerName);
                    KafkaClusterProperties.BatchConfig batch = consumer.getBatch();
                    if (!batch.isEnabled()) {
                        return new IdempotentConsumer(consumerName, cluster, store,
                                handlerRegistry.getHandler(consumerName), tracker,
                                props.resolveAckPolicy(consumer), gate);
                    }
                    if (batch.getMode() == KafkaClusterProperties.BatchConfig.Mode.STANDARD) {
                        return new BatchPassThroughConsumer(consumerName, cluster,
                                handlerRegistry.getEnvelopeHandler(consumerName), tracker,
                                props.resolveAckPolicy(consumer), gate);
                    }
                    return new BatchIdempotentConsumer(consumerName, cluster, store,
                            handlerRegistry.getBatchHandler(consumerName), tracker,
                            props.resolveAckMode(consumer), gate);
                });

                registry.registerBeanDefinition(beanName, beanDef);
            }
        }
    }
}
