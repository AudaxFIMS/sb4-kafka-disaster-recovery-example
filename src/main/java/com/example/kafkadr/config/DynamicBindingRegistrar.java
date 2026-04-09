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

        Map<String, Object> generated = new LinkedHashMap<>();
        List<String> functionNames = new ArrayList<>();

        String primaryCluster = props.getClusters().entrySet().stream()
                .min(Comparator.comparingInt(e -> e.getValue().getPriority()))
                .map(Map.Entry::getKey)
                .orElseThrow();

        generateBinders(props, generated);
        generateConsumerBindings(props, generated, functionNames, primaryCluster);
        generateProducerBindings(props, generated);

        if (!functionNames.isEmpty()) {
            generated.put("spring.cloud.function.definition", String.join(";", functionNames));
        }

        environment.getPropertySources().addFirst(
                new MapPropertySource("kafka-dr-dynamic-bindings", generated));

        log.info("Generated {} consumer bindings, {} producer configs for {} clusters",
                functionNames.size(), props.getProducers().size(), props.getClusters().size());

        registerConsumerBeans(registry, props);
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

    private void generateConsumerBindings(KafkaClusterProperties props,
                                          Map<String, Object> generated,
                                          List<String> functionNames,
                                          String primaryCluster) {
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

                if ("native".equalsIgnoreCase(consumer.getContentType())) {
                    generated.put(prefix + ".consumer.use-native-decoding", "true");
                }

                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + bindingName
                        + ".consumer.configuration";
                for (Map.Entry<String, String> prop : consumer.getProperties().entrySet()) {
                    generated.put(kafkaPrefix + "." + prop.getKey(), prop.getValue());
                }
            }
        }
    }

    /**
     * Generates output binding properties for each producer topic.
     *
     * StreamBridge.send(topic, binderName, message) resolves output binding name as the topic name.
     * We generate properties for "{topic}-out-0" which is what Spring Cloud Stream uses internally.
     * Additionally, we generate for just "{topic}" to cover StreamBridge's dynamic binding resolution.
     */
    private void generateProducerBindings(KafkaClusterProperties props, Map<String, Object> generated) {
        for (KafkaClusterProperties.ProducerConfig producer : props.getProducers()) {
            String topic = producer.getTopic();

            // StreamBridge creates bindings with name = destination (the topic name)
            // Generate for both possible binding name patterns
            for (String outBinding : List.of(topic, topic + "-out-0")) {
                String prefix = "spring.cloud.stream.bindings." + outBinding;
                generated.put(prefix + ".destination", topic);

                if ("native".equalsIgnoreCase(producer.getContentType())) {
                    generated.put(prefix + ".producer.use-native-encoding", "true");
                }

                String kafkaPrefix = "spring.cloud.stream.kafka.bindings." + outBinding
                        + ".producer.configuration";
                for (Map.Entry<String, String> prop : producer.getProperties().entrySet()) {
                    generated.put(kafkaPrefix + "." + prop.getKey(), prop.getValue());
                }
            }

            log.info("Generated producer binding: {} (content-type={})", topic, producer.getContentType());
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
                log.info("Registered consumer bean: {} (topic={}, cluster={})", beanName, topic, cluster);
            }
        }
    }
}
