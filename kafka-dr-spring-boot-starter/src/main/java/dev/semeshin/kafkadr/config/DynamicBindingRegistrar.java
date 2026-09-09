package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.consumer.*;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
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

        if (props == null || props.getClusters().isEmpty()) {
            log.warn("kafka-dr.enabled=true but no clusters configured, skipping");
            return;
        }

        // Remove Spring Boot's default KafkaAdmin to prevent it from connecting
        // to localhost:9092 and blocking startup. DR manages its own AdminClients.
        if (registry.containsBeanDefinition("kafkaAdmin")) {
            registry.removeBeanDefinition("kafkaAdmin");
        }

        // Static utility, so the flag has to be pushed in — and before the first probe.
        KafkaAdminHelper.setDebugEnabled(props.getDebug().isEnable());

        ConsumerConfigValidator.validate(props);

        Set<String> reachableClusters = probeAllClusters(props);
        log.info("Reachable clusters at startup: {}", reachableClusters);

        Map<String, Object> generated = new LinkedHashMap<>();
        List<String> functionNames = new ArrayList<>();

        // Generate binder configs for ALL clusters (just environment properties)
        generateBinders(props, generated);
        // Generate consumer binding properties for ALL clusters
        generateConsumerBindingProperties(props, generated);
        // But only include reachable clusters in function definition
        // (unreachable clusters get bindings created later by LateBindingInitializer)
        generateFunctionDefinitions(props, functionNames, reachableClusters);
        generateProducerBindings(props, generated);

        if (!functionNames.isEmpty()) {
            generated.put("spring.cloud.function.definition", String.join(";", functionNames));
        }

        environment.getPropertySources().addFirst(
                new MapPropertySource("kafka-dr-dynamic-bindings", generated));

        log.info("Generated bindings for {} reachable clusters, properties for all {} clusters",
                reachableClusters.size(), props.getClusters().size());

        if (props.isAutoCreateTopics()) {
            for (String cluster : reachableClusters) {
                String brokers = props.getClusters().get(cluster).getBootstrapServers();
                KafkaAdminHelper.provisionTopics(cluster, brokers, props);
            }
        }

        // Register function beans for ALL clusters (needed for late binding)
        registerConsumerBeans(registry, props);

        // Store reachable clusters in environment so StartupClusterState can read them
        generated.put("kafka-dr.internal.initialized-clusters", String.join(",", reachableClusters));
    }

    private static void putIfNotNull(Map<String, Object> generated, String key, Object value) {
        if (value != null) {
            generated.put(key, value.toString());
        }
    }

    private Set<String> probeAllClusters(KafkaClusterProperties props) {
        Set<String> reachable = new LinkedHashSet<>();
        for (String name : props.getClusters().keySet()) {
            if (KafkaAdminHelper.probeCluster(name, props)) {
                reachable.add(name);
            } else {
                log.warn("[{}] Unreachable at startup — will be initialized later", name);
            }
        }
        return reachable;
    }

    private void generateBinders(KafkaClusterProperties props, Map<String, Object> generated) {
        for (Map.Entry<String, KafkaClusterProperties.ClusterConfig> entry : props.getClusters().entrySet()) {
            String clusterName = entry.getKey();
            String binderPrefix = "spring.cloud.stream.binders." + clusterName;
            String envPrefix = binderPrefix + ".environment";

            generated.put(binderPrefix + ".type", "kafka");
            generated.put(envPrefix + ".spring.cloud.stream.kafka.binder.brokers",
                    entry.getValue().getBootstrapServers());

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
    private void generateConsumerBindingProperties(KafkaClusterProperties props, Map<String, Object> generated) {
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers().values()) {
            String consumerName = consumer.getName();
            String topic = consumer.getTopic();

            for (String cluster : props.getClusters().keySet()) {
                String functionName = KafkaClusterProperties.functionName(consumerName, cluster);
                String bindingName = functionName + "-in-0";
                String prefix = "spring.cloud.stream.bindings." + bindingName;

                generated.put(prefix + ".destination", topic);
                generated.put(prefix + ".group", consumer.getGroup());
                generated.put(prefix + ".binder", cluster);
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
    private void generateFunctionDefinitions(KafkaClusterProperties props,
                                             List<String> functionNames,
                                             Set<String> reachableClusters) {
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers().values()) {
            for (String cluster : reachableClusters) {
                functionNames.add(KafkaClusterProperties.functionName(consumer.getName(), cluster));
            }
        }
    }

    private void generateProducerBindings(KafkaClusterProperties props, Map<String, Object> generated) {
        for (KafkaClusterProperties.ProducerConfig producer : props.getProducers().values()) {
            String producerName = producer.getName();
            String topic = producer.getTopic();
            String bindingName = KafkaClusterProperties.producerBindingName(producerName);

            for (String outBinding : List.of(bindingName, bindingName + "-out-0")) {
                String prefix = "spring.cloud.stream.bindings." + outBinding;
                generated.put(prefix + ".destination", topic);

                if ("native".equalsIgnoreCase(producer.getContentType())) {
                    generated.put(prefix + ".producer.use-native-encoding", "true");
                }

                String corePrefix = prefix + ".producer";
                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + outBinding + ".producer";

                for (Map.Entry<String, String> prop : props.getEffectiveProducerProperties(producer).entrySet()) {
                    BindingPropertyRouter.checkNotReserved(prop.getKey(), producerName, true);
                    String target = BindingPropertyRouter.forProducerKey(prop.getKey(), producerName)
                            == BindingPropertyRouter.Namespace.CORE ? corePrefix : kafkaPrefix;
                    generated.put(target + "." + prop.getKey(), prop.getValue());
                }
            }
        }
    }

    private void registerConsumerBeans(BeanDefinitionRegistry registry, KafkaClusterProperties props) {
        if (props.getConsumers().isEmpty()) return;

        BeanFactory beanFactory = (BeanFactory) registry;

        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers().values()) {
            String consumerName = consumer.getName();
            for (String cluster : props.getClusters().keySet()) {
                String beanName = KafkaClusterProperties.functionName(consumerName, cluster);

                GenericBeanDefinition beanDef = new GenericBeanDefinition();
                beanDef.setBeanClass(Consumer.class);
                beanDef.setInstanceSupplier(() -> {
                    // Master switch: with idempotency disabled the store bean is never
                    // looked up, so even a user-defined store (e.g. Redis) is ignored.
                    IdempotencyStore store = props.isIdempotencyEnabled(consumer)
                            ? beanFactory.getBean(IdempotencyStore.class)
                            : IdempotencyStore.DISABLED;
                    MessageHandlerRegistry handlerRegistry = beanFactory.getBean(MessageHandlerRegistry.class);
                    LastProcessedTimestampTracker tracker = beanFactory.getBean(LastProcessedTimestampTracker.class);
                    KafkaClusterProperties.BatchConfig batch = consumer.getBatch();
                    if (!batch.isEnabled()) {
                        return new IdempotentConsumer(consumerName, cluster, store,
                                handlerRegistry.getHandler(consumerName), tracker,
                                props.resolveAckPolicy(consumer));
                    }
                    if (batch.getMode() == KafkaClusterProperties.BatchConfig.Mode.STANDARD) {
                        return new BatchPassThroughConsumer(consumerName, cluster,
                                handlerRegistry.getEnvelopeHandler(consumerName), tracker,
                                props.resolveAckPolicy(consumer));
                    }
                    return new BatchIdempotentConsumer(consumerName, cluster, store,
                            handlerRegistry.getBatchHandler(consumerName), tracker,
                            props.resolveAckMode(consumer));
                });

                registry.registerBeanDefinition(beanName, beanDef);
            }
        }
    }
}
