package com.example.kafkadr.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "kafka-dr")
public class KafkaClusterProperties {

    private Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
    private List<ConsumerConfig> consumers = List.of();

    /**
     * Default binder environment properties applied to ALL clusters.
     * Structure mirrors spring.cloud.stream.binders.{name}.environment
     * Per-cluster settings override these defaults.
     */
    private Map<String, Object> defaultEnvironment = new LinkedHashMap<>();

    private HealthCheckConfig healthCheck = new HealthCheckConfig();
    private IdempotencyConfig idempotency = new IdempotencyConfig();

    public Map<String, ClusterConfig> getClusters() { return clusters; }
    public void setClusters(Map<String, ClusterConfig> clusters) { this.clusters = clusters; }
    public List<ConsumerConfig> getConsumers() { return consumers; }
    public void setConsumers(List<ConsumerConfig> consumers) { this.consumers = consumers; }
    public Map<String, Object> getDefaultEnvironment() { return defaultEnvironment; }
    public void setDefaultEnvironment(Map<String, Object> defaultEnvironment) { this.defaultEnvironment = defaultEnvironment; }
    public HealthCheckConfig getHealthCheck() { return healthCheck; }
    public void setHealthCheck(HealthCheckConfig healthCheck) { this.healthCheck = healthCheck; }
    public IdempotencyConfig getIdempotency() { return idempotency; }
    public void setIdempotency(IdempotencyConfig idempotency) { this.idempotency = idempotency; }

    public static class ClusterConfig {
        private String bootstrapServers;
        private int priority = 100;

        /**
         * Per-cluster binder environment properties.
         * Structure mirrors spring.cloud.stream.binders.{name}.environment
         * Merged on top of kafka-dr.default-environment.
         */
        private Map<String, Object> environment = new LinkedHashMap<>();

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }
        public Map<String, Object> getEnvironment() { return environment; }
        public void setEnvironment(Map<String, Object> environment) { this.environment = environment; }
    }

    public static class ConsumerConfig {
        private String topic;
        private String group = "dr-default-group";
        private String handler;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getGroup() { return group; }
        public void setGroup(String group) { this.group = group; }
        public String getHandler() { return handler; }
        public void setHandler(String handler) { this.handler = handler; }
    }

    public static class HealthCheckConfig {
        private long intervalMs = 5000;
        private long timeoutMs = 3000;
        private int failureThreshold = 3;
        private int recoveryThreshold = 3;

        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public long getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
        public int getRecoveryThreshold() { return recoveryThreshold; }
        public void setRecoveryThreshold(int recoveryThreshold) { this.recoveryThreshold = recoveryThreshold; }
    }

    public static class IdempotencyConfig {
        private long ttlSeconds = 3600;
        private String keyPrefix = "idempotency";

        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
        public String getKeyPrefix() { return keyPrefix; }
        public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
    }

    /**
     * Returns merged environment for a cluster: default-environment + per-cluster overrides.
     * Flattened to dot-notation keys.
     */
    public Map<String, String> getEffectiveEnvironment(String clusterName) {
        Map<String, String> merged = new LinkedHashMap<>();
        flatten("", defaultEnvironment, merged);
        ClusterConfig cluster = clusters.get(clusterName);
        if (cluster != null) {
            flatten("", cluster.getEnvironment(), merged);
        }
        return merged;
    }

    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> source, Map<String, String> target) {
        if (source == null) return;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flatten(key, (Map<String, Object>) value, target);
            } else if (value != null) {
                target.put(key, value.toString());
            }
        }
    }

    // --- Naming utilities ---

    public static String functionName(String topic, String cluster) {
        return toCamelCase(topic) + capitalize(cluster);
    }

    public static String bindingName(String topic, String cluster) {
        return functionName(topic, cluster) + "-in-0";
    }

    private static String toCamelCase(String dashed) {
        String[] parts = dashed.split("-");
        StringBuilder sb = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            sb.append(capitalize(parts[i]));
        }
        return sb.toString();
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}
