package com.example.kafkadr.routing;

import com.example.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Manages both consumer (input) and producer (output) bindings on cluster switch.
 * - Stops/starts consumer bindings via BindingsLifecycleController
 * - Clears StreamBridge's cached output channels so dead producers stop reconnecting
 */
@Component
public class BindingLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(BindingLifecycleManager.class);

    private final BindingsLifecycleController bindingsController;
    private final StreamBridge streamBridge;
    private final Map<String, List<String>> inputBindingsByCluster;

    public BindingLifecycleManager(BindingsLifecycleController bindingsController,
                                   StreamBridge streamBridge,
                                   KafkaClusterProperties properties) {
        this.bindingsController = bindingsController;
        this.streamBridge = streamBridge;
        this.inputBindingsByCluster = buildInputBindingIndex(properties);
        log.info("Input bindings by cluster: {}", inputBindingsByCluster);
    }

    @EventListener
    public void onClusterSwitched(ClusterSwitchedEvent event) {
        String previous = event.getPreviousCluster();
        String next = event.getNewCluster();

        log.info("Switching bindings: '{}' -> '{}'", previous, next);

        stopInputBindings(previous);
        clearProducerCache(previous);
        startInputBindings(next);
    }

    private void stopInputBindings(String cluster) {
        for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
            try {
                bindingsController.changeState(binding, State.STOPPED);
                log.info("Stopped input binding: {}", binding);
            } catch (Exception e) {
                log.error("Failed to stop input binding '{}': {}", binding, e.getMessage());
            }
        }
    }

    private void startInputBindings(String cluster) {
        for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
            try {
                bindingsController.changeState(binding, State.STARTED);
                log.info("Started input binding: {}", binding);
            } catch (Exception e) {
                log.error("Failed to start input binding '{}': {}", binding, e.getMessage());
            }
        }
    }

    /**
     * Clears StreamBridge's internal channel cache for entries belonging to the given binder.
     * This closes the underlying Kafka producers so they stop reconnecting to the dead cluster.
     */
    private void clearProducerCache(String cluster) {
        try {
            Field channelCacheField = StreamBridge.class.getDeclaredField("channelCache");
            channelCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, ?> channelCache = (Map<String, ?>) channelCacheField.get(streamBridge);

            if (channelCache == null || channelCache.isEmpty()) {
                return;
            }

            List<String> keysToRemove = channelCache.keySet().stream()
                    .filter(key -> key.contains(":" + cluster) || key.endsWith(cluster))
                    .toList();

            for (String key : keysToRemove) {
                channelCache.remove(key);
                log.info("Removed cached producer channel: {}", key);
            }

            if (keysToRemove.isEmpty()) {
                log.debug("No cached producer channels found for cluster '{}'", cluster);
            }
        } catch (NoSuchFieldException e) {
            log.warn("StreamBridge.channelCache field not found — producer cache cleanup skipped. "
                    + "This may happen with a different Spring Cloud Stream version.");
        } catch (Exception e) {
            log.error("Failed to clear producer cache for cluster '{}': {}", cluster, e.getMessage());
        }
    }

    private Map<String, List<String>> buildInputBindingIndex(KafkaClusterProperties properties) {
        Map<String, List<String>> index = new HashMap<>();

        for (String cluster : properties.getClusters().keySet()) {
            List<String> bindings = new ArrayList<>();
            for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers()) {
                bindings.add(KafkaClusterProperties.bindingName(consumer.getTopic(), cluster));
            }
            index.put(cluster, bindings);
        }

        return index;
    }
}
