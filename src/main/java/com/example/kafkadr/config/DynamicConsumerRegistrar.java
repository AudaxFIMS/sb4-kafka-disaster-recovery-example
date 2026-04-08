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
public class DynamicConsumerRegistrar implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DynamicConsumerRegistrar.class);

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

        if (props == null || props.getConsumers().isEmpty() || props.getClusters().isEmpty()) {
            log.warn("No kafka-dr consumers or clusters configured, skipping dynamic registration");
            return;
        }

        injectStreamProperties(props);
        registerConsumerBeans(registry, props);
    }

    private void injectStreamProperties(KafkaClusterProperties props) {
        Map<String, Object> generated = new LinkedHashMap<>();
        List<String> functionNames = new ArrayList<>();

        String primaryCluster = props.getClusters().entrySet().stream()
                .min(Comparator.comparingInt(e -> e.getValue().getPriority()))
                .map(Map.Entry::getKey)
                .orElseThrow();

        // Generate binder definitions with full environment
        for (Map.Entry<String, KafkaClusterProperties.ClusterConfig> entry : props.getClusters().entrySet()) {
            String clusterName = entry.getKey();
            String binderPrefix = "spring.cloud.stream.binders." + clusterName;
            String envPrefix = binderPrefix + ".environment";

            generated.put(binderPrefix + ".type", "kafka");
            generated.put(envPrefix + ".spring.cloud.stream.kafka.binder.brokers",
                    entry.getValue().getBootstrapServers());

            // Merge default-environment + per-cluster environment, flattened
            Map<String, String> effectiveEnv = props.getEffectiveEnvironment(clusterName);
            for (Map.Entry<String, String> envEntry : effectiveEnv.entrySet()) {
                generated.put(envPrefix + "." + envEntry.getKey(), envEntry.getValue());
            }
        }

        // Generate binding definitions
        for (KafkaClusterProperties.ConsumerConfig consumer : props.getConsumers()) {
            String topic = consumer.getTopic();

            for (String cluster : props.getClusters().keySet()) {
                String functionName = KafkaClusterProperties.functionName(topic, cluster);
                String bindingName = functionName + "-in-0";
                String prefix = "spring.cloud.stream.bindings." + bindingName;

                functionNames.add(functionName);
                generated.put(prefix + ".destination", topic);
                generated.put(prefix + ".group", consumer.getGroup());
                generated.put(prefix + ".binder", cluster);

                if (!cluster.equals(primaryCluster)) {
                    generated.put(prefix + ".consumer.auto-startup", "false");
                }
            }
        }

        String functionDef = String.join(";", functionNames);
        generated.put("spring.cloud.function.definition", functionDef);

        environment.getPropertySources().addFirst(
                new MapPropertySource("kafka-dr-dynamic-bindings", generated));

        log.info("Generated function definition: {}", functionDef);
        log.info("Generated {} binding properties for {} clusters",
                functionNames.size(), props.getClusters().size());
    }

    private void registerConsumerBeans(BeanDefinitionRegistry registry, KafkaClusterProperties props) {
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
                log.info("Registered consumer bean: {} (topic={}, cluster={})", beanName, topic, cluster);
            }
        }
    }
}
