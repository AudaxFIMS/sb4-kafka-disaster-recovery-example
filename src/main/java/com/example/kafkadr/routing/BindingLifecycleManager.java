package com.example.kafkadr.routing;

import com.example.kafkadr.config.KafkaClusterProperties;
import com.example.kafkadr.config.StartupClusterState;
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
 * Supports both startup-initialized and late-initialized clusters.
 */
@Component
public class BindingLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(BindingLifecycleManager.class);

    private final BindingsLifecycleController bindingsController;
    private final StreamBridge streamBridge;
    private final StartupClusterState startupState;
    private final LateBindingInitializer lateBindingInitializer;
    private final Map<String, List<String>> inputBindingsByCluster;
    private final Set<String> startupClusters;

    public BindingLifecycleManager(BindingsLifecycleController bindingsController,
                                   StreamBridge streamBridge,
                                   KafkaClusterProperties properties,
                                   StartupClusterState startupState,
                                   LateBindingInitializer lateBindingInitializer) {
        this.bindingsController = bindingsController;
        this.streamBridge = streamBridge;
        this.startupState = startupState;
        this.lateBindingInitializer = lateBindingInitializer;
        this.startupClusters = Set.copyOf(startupState.getInitializedClusters());
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
        if (startupClusters.contains(cluster)) {
            // Startup-initialized: use BindingsLifecycleController
            for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
                try {
                    bindingsController.changeState(binding, State.STOPPED);
                    log.info("Stopped input binding: {}", binding);
                } catch (Exception e) {
                    log.error("Failed to stop input binding '{}': {}", binding, e.getMessage());
                }
            }
        } else if (startupState.isInitialized(cluster)) {
            // Late-initialized: use LateBindingInitializer
            lateBindingInitializer.stopBindings(cluster);
        }
    }

    private void startInputBindings(String cluster) {
        if (!startupState.isInitialized(cluster)) {
            log.warn("Cluster '{}' not yet initialized, cannot start bindings", cluster);
            return;
        }

        if (startupClusters.contains(cluster)) {
            // Startup-initialized: use BindingsLifecycleController
            for (String binding : inputBindingsByCluster.getOrDefault(cluster, List.of())) {
                try {
                    bindingsController.changeState(binding, State.STARTED);
                    log.info("Started input binding: {}", binding);
                } catch (Exception e) {
                    log.error("Failed to start input binding '{}': {}", binding, e.getMessage());
                }
            }
        } else {
            // Late-initialized: use LateBindingInitializer
            lateBindingInitializer.startBindings(cluster);
        }
    }

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
        } catch (NoSuchFieldException e) {
            log.warn("StreamBridge.channelCache field not found — producer cache cleanup skipped.");
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
