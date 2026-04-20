package com.example.kafkadr.config;

import com.example.kafkadr.consumer.IdempotentConsumer;
import com.example.kafkadr.consumer.MessageHandlerRegistry;
import com.example.kafkadr.idempotency.IdempotencyStore;
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

@Component
public class DynamicBindingRegistrar implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DynamicBindingRegistrar.class);

    private ConfigurableEnvironment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = (ConfigurableEnvironment) environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        KafkaClusterProperties props = Binder.get(environment)
                .bind("kafka-dr", KafkaClusterProperties.class)
                .orElse(null);

        if (props == null || props.getClusters().isEmpty()) {
            log.warn("No kafka-dr clusters configured, skipping");
            return;
        }

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

    private Set<String> probeAllClusters(KafkaClusterProperties props) {
        Set<String> reachable = new LinkedHashSet<>();
        for (String name : props.getClusters().keySet()) {
            if (KafkaAdminHelper.probeCluster(name, props)) {
                reachable.add(name);
            } else {
                log.warn("Cluster '{}' unreachable at startup — will be initialized when it comes online", name);
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
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers()) {
            String topic = consumer.getTopic();

            for (String cluster : props.getClusters().keySet()) {
                String functionName = KafkaClusterProperties.functionName(topic, cluster);
                String bindingName = functionName + "-in-0";
                String prefix = "spring.cloud.stream.bindings." + bindingName;

                generated.put(prefix + ".destination", topic);
                generated.put(prefix + ".group", consumer.getGroup());
                generated.put(prefix + ".binder", cluster);
                generated.put(prefix + ".consumer.auto-startup", "false");

                if ("native".equalsIgnoreCase(consumer.getContentType())) {
                    generated.put(prefix + ".consumer.use-native-decoding", "true");
                }

                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + bindingName + ".consumer";
                for (Map.Entry<String, String> prop : props.getEffectiveConsumerProperties(consumer).entrySet()) {
                    generated.put(kafkaPrefix + "." + prop.getKey(), prop.getValue());
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
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers()) {
            for (String cluster : reachableClusters) {
                functionNames.add(KafkaClusterProperties.functionName(consumer.getTopic(), cluster));
            }
        }
    }

    private void generateProducerBindings(KafkaClusterProperties props, Map<String, Object> generated) {
        for (KafkaClusterProperties.ProducerConfig producer : props.getProducers()) {
            String topic = producer.getTopic();

            for (String outBinding : List.of(topic, topic + "-out-0")) {
                String prefix = "spring.cloud.stream.bindings." + outBinding;
                generated.put(prefix + ".destination", topic);

                if ("native".equalsIgnoreCase(producer.getContentType())) {
                    generated.put(prefix + ".producer.use-native-encoding", "true");
                }

                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + outBinding + ".producer";
                for (Map.Entry<String, String> prop : props.getEffectiveProducerProperties(producer).entrySet()) {
                    generated.put(kafkaPrefix + "." + prop.getKey(), prop.getValue());
                }
            }
        }
    }

    private void registerConsumerBeans(BeanDefinitionRegistry registry, KafkaClusterProperties props) {
        if (props.getConsumers().isEmpty()) return;

        BeanFactory beanFactory = (BeanFactory) registry;

        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers()) {
            String topic = consumer.getTopic();
            for (String cluster : props.getClusters().keySet()) {
                String beanName = KafkaClusterProperties.functionName(topic, cluster);

                GenericBeanDefinition beanDef = new GenericBeanDefinition();
                beanDef.setBeanClass(Consumer.class);
                beanDef.setInstanceSupplier(() -> {
                    IdempotencyStore store = beanFactory.getBean(IdempotencyStore.class);
                    MessageHandlerRegistry handlerRegistry = beanFactory.getBean(MessageHandlerRegistry.class);
                    return new IdempotentConsumer(topic, cluster, store, handlerRegistry.getHandler(topic));
                });

                registry.registerBeanDefinition(beanName, beanDef);
            }
        }
    }
}
