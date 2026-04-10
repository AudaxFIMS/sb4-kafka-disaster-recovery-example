package com.example.kafkadr.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which clusters have been initialized (binder + bindings created).
 * Populated at startup from environment property set by DynamicBindingRegistrar,
 * and updated at runtime by LateBindingInitializer.
 */
@Component
public class StartupClusterState {

    private final Set<String> initializedClusters = ConcurrentHashMap.newKeySet();

    @Value("${kafka-dr.internal.initialized-clusters:}")
    private String initializedClustersProperty;

    @PostConstruct
    void init() {
        if (initializedClustersProperty != null && !initializedClustersProperty.isBlank()) {
            initializedClusters.addAll(Arrays.asList(initializedClustersProperty.split(",")));
        }
    }

    public void addInitializedCluster(String cluster) {
        initializedClusters.add(cluster);
    }

    public Set<String> getInitializedClusters() {
        return Set.copyOf(initializedClusters);
    }

    public boolean isInitialized(String cluster) {
        return initializedClusters.contains(cluster);
    }
}
