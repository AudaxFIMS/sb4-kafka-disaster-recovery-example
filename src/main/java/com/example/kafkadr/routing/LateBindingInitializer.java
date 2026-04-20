package com.example.kafkadr.routing;

import com.example.kafkadr.config.KafkaAdminHelper;
import com.example.kafkadr.config.KafkaClusterProperties;
import com.example.kafkadr.config.StartupClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final StartupClusterState startupState;
    private final ActiveClusterManager clusterManager;
    private final BinderFactory binderFactory;
    private final BindingServiceProperties bindingServiceProperties;
    private final Map<String, Consumer<Message<?>>> functionBeans;
    private final Map<String, Binding<?>> lateBindings = new ConcurrentHashMap<>();

    public LateBindingInitializer(KafkaClusterProperties properties,
                                  StartupClusterState startupState,
                                  ActiveClusterManager clusterManager,
                                  BinderFactory binderFactory,
                                  BindingServiceProperties bindingServiceProperties,
                                  ApplicationContext applicationContext) {
        this.properties = properties;
        this.startupState = startupState;
        this.clusterManager = clusterManager;
        this.binderFactory = binderFactory;
        this.bindingServiceProperties = bindingServiceProperties;

        // Collect all consumer function beans
        this.functionBeans = new ConcurrentHashMap<>();
        for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers()) {
            for (String cluster : properties.getClusters().keySet()) {
                String beanName = KafkaClusterProperties.functionName(consumer.getTopic(), cluster);
                try {
                    @SuppressWarnings("unchecked")
                    Consumer<Message<?>> bean =
                            applicationContext.getBean(beanName, Consumer.class);
                    functionBeans.put(beanName, bean);
                } catch (Exception e) {
                    log.debug("Function bean '{}' not found: {}", beanName, e.getMessage());
                }
            }
        }
    }

    @Scheduled(fixedDelayString = "${kafka-dr.late-initializer.timeout-ms:5000}")
    public void checkAndInitializeClusters() {
        for (String cluster : properties.getClusters().keySet()) {
            if (startupState.isInitialized(cluster)) {
                continue;
            }

            if (!KafkaAdminHelper.probeCluster(cluster, properties)) {
                continue;
            }

            String brokers = properties.getClusters().get(cluster).getBootstrapServers();
            try {
                if (properties.isAutoCreateTopics()) {
                    KafkaAdminHelper.provisionTopics(cluster, brokers, properties);
                }
                initializeCluster(cluster);
                startupState.addInitializedCluster(cluster);
                log.info(">>> Late-initialized cluster '{}' — bindings created <<<", cluster);

                // If this cluster is already active (switch happened before bindings existed), start consumers now
                if (cluster.equals(clusterManager.getActiveCluster())) {
                    startBindings(cluster);
                    log.info(">>> Started late bindings for already-active cluster '{}' <<<", cluster);
                }
            } catch (Exception e) {
                log.error("Failed to late-initialize cluster '{}': {}", cluster, e.getMessage(), e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initializeCluster(String cluster) {
        log.info("Late-initializing cluster '{}'...", cluster);

        Binder<MessageChannel, ? extends ConsumerProperties, ?> binder =
                (Binder<MessageChannel, ? extends ConsumerProperties, ?>)
                        binderFactory.getBinder(cluster, MessageChannel.class);

        for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers()) {
            String topic = consumer.getTopic();
            String functionName = KafkaClusterProperties.functionName(topic, cluster);
            String bindingName = functionName + "-in-0";

            Consumer<Message<?>> handler = functionBeans.get(functionName);
            if (handler == null) {
                log.warn("No function bean found for '{}', skipping", functionName);
                continue;
            }

            // Create input channel and wire to handler
            DirectChannel channel = new DirectChannel();
            channel.subscribe(message -> {
                try {
                    handler.accept(message);
                } catch (Exception e) {
                    log.error("Error in late-bound consumer {}: {}", functionName, e.getMessage());
                }
            });

            // Get binding properties from environment
            var bindingProps = bindingServiceProperties.getBindingProperties(bindingName);
            String group = bindingProps.getGroup();
            String destination = bindingProps.getDestination();
            if (destination == null) destination = topic;
            if (group == null) group = consumer.getGroup();

            // Create extended consumer properties required by Kafka binder
            var kafkaConsumerProps = new KafkaConsumerProperties();

            // Apply per-consumer Kafka client properties (deserializer, etc.)
            Map<String, String> effectiveProps = properties.getEffectiveConsumerProperties(consumer);
            for (Map.Entry<String, String> entry : effectiveProps.entrySet()) {
                if (entry.getKey().startsWith("configuration.")) {
                    kafkaConsumerProps.getConfiguration().put(
                            entry.getKey().substring("configuration.".length()), entry.getValue());
                }
            }

            var extendedProps = new ExtendedConsumerProperties<>(kafkaConsumerProps);
            extendedProps.setAutoStartup(false);

            // Enable native decoding if content-type is native
            if ("native".equalsIgnoreCase(consumer.getContentType())) {
                extendedProps.setUseNativeDecoding(true);
            }

            // Bind
            var binding = ((Binder<MessageChannel, ExtendedConsumerProperties<KafkaConsumerProperties>, ?>) binder)
                    .bindConsumer(destination, group, channel, extendedProps);

            lateBindings.put(bindingName, binding);
            log.info("Created late binding: {} (topic={}, cluster={})", bindingName, topic, cluster);
        }
    }

    /**
     * Start consumer bindings for a late-initialized cluster.
     * Called by BindingLifecycleManager when switching to this cluster.
     */
    public void startBindings(String cluster) {
        for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers()) {
            String bindingName = KafkaClusterProperties.bindingName(consumer.getTopic(), cluster);
            var binding = lateBindings.get(bindingName);
            if (binding != null) {
                binding.start();
                log.info("Started late binding: {}", bindingName);
            }
        }
    }

    /**
     * Stop consumer bindings for a late-initialized cluster.
     */
    public void stopBindings(String cluster) {
        for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers()) {
            String bindingName = KafkaClusterProperties.bindingName(consumer.getTopic(), cluster);
            var binding = lateBindings.get(bindingName);
            if (binding != null) {
                binding.stop();
                log.info("Stopped late binding: {}", bindingName);
            }
        }
    }

}
