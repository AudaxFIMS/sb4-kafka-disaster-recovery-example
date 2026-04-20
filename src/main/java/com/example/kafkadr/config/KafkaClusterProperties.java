package com.example.kafkadr.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
@ConfigurationProperties(prefix = "kafka-dr")
public class KafkaClusterProperties {

    private Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
    private List<ConsumerConfig> consumers = List.of();
    private List<ProducerConfig> producers = List.of();
    private Map<String, Object> defaultEnvironment = new LinkedHashMap<>();
    private Map<String, Object> defaultConsumerProperties = new LinkedHashMap<>();
    private Map<String, Object> defaultProducerProperties = new LinkedHashMap<>();
    private boolean autoCreateTopics = false;
    private HealthCheckConfig healthCheck = new HealthCheckConfig();
    private IdempotencyConfig idempotency = new IdempotencyConfig();

    public Map<String, ClusterConfig> getClusters() { return clusters; }
    public void setClusters(Map<String, ClusterConfig> clusters) { this.clusters = clusters; }
    public List<ConsumerConfig> getConsumers() { return consumers; }
    public void setConsumers(List<ConsumerConfig> consumers) { this.consumers = consumers; }
    public List<ProducerConfig> getProducers() { return producers; }
    public void setProducers(List<ProducerConfig> producers) { this.producers = producers; }
    public Map<String, Object> getDefaultEnvironment() { return defaultEnvironment; }
    public void setDefaultEnvironment(Map<String, Object> defaultEnvironment) { this.defaultEnvironment = defaultEnvironment; }
    public Map<String, Object> getDefaultConsumerProperties() { return defaultConsumerProperties; }
    public void setDefaultConsumerProperties(Map<String, Object> defaultConsumerProperties) { this.defaultConsumerProperties = defaultConsumerProperties; }
    public Map<String, Object> getDefaultProducerProperties() { return defaultProducerProperties; }
    public void setDefaultProducerProperties(Map<String, Object> defaultProducerProperties) { this.defaultProducerProperties = defaultProducerProperties; }
    public boolean isAutoCreateTopics() { return autoCreateTopics; }
    public void setAutoCreateTopics(boolean autoCreateTopics) { this.autoCreateTopics = autoCreateTopics; }
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

        /**
         * Payload content type. Controls how byte[] from Kafka is converted
         * to the handler's Message<T> payload type.
         * - "json"   — deserialize via Jackson ObjectMapper (default)
         * - "string" — convert byte[] to String (UTF-8)
         * - "bytes"  — pass byte[] as-is, no conversion
         * - "native" — Kafka deserializer already produced the target type (e.g. Avro),
         *              skip conversion entirely
         */
        private String contentType = "json";

        /**
         * Per-consumer properties, merged on top of kafka-dr.default-consumer-properties.
         * Supports both Spring Cloud Stream consumer properties
         * and Kafka client properties under "configuration" key (e.g. configuration.value.deserializer).
         * Maps to spring.cloud.stream.kafka.bindings.{binding}.consumer.*
         */
        private Map<String, Object> properties = new LinkedHashMap<>();

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getGroup() { return group; }
        public void setGroup(String group) { this.group = group; }
        public String getHandler() { return handler; }
        public void setHandler(String handler) { this.handler = handler; }
        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
    }

    /**
     * Defines a producer for a specific topic.
     * Output binding properties are generated for each producer x cluster pair.
     */
    public static class ProducerConfig {
        private String topic;

        /**
         * Serialization type:
         * - "json"   — default Spring serialization (default)
         * - "string" — StringSerializer
         * - "bytes"  — ByteArraySerializer
         * - "native" — custom Kafka serializer (e.g. Avro), enable useNativeEncoding
         */
        private String contentType = "json";

        /**
         * Per-producer properties, merged on top of kafka-dr.default-producer-properties.
         * Supports both Spring Cloud Stream producer properties (e.g. sync)
         * and Kafka client properties under "configuration" key (e.g. configuration.acks).
         * Maps to spring.cloud.stream.kafka.bindings.{binding}.producer.*
         */
        private Map<String, Object> properties = new LinkedHashMap<>();

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
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
        private String type = "in-memory";
        private long ttlSeconds = 3600;
        private String keyPrefix = "idempotency";

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
        public String getKeyPrefix() { return keyPrefix; }
        public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
    }

    /**
     * Returns merged consumer properties: default-consumer-properties + per-consumer overrides.
     * Flattened to dot-notation keys.
     */
    public Map<String, String> getEffectiveConsumerProperties(ConsumerConfig consumer) {
        Map<String, String> merged = new LinkedHashMap<>();
        flatten("", defaultConsumerProperties, merged);
        flatten("", consumer.getProperties(), merged);
        return merged;
    }

    /**
     * Returns merged producer properties: default-producer-properties + per-producer overrides.
     * Flattened to dot-notation keys.
     */
    public Map<String, String> getEffectiveProducerProperties(ProducerConfig producer) {
        Map<String, String> merged = new LinkedHashMap<>();
        flatten("", defaultProducerProperties, merged);
        flatten("", producer.getProperties(), merged);
        return merged;
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

    private static String toCamelCase(String name) {
        String[] parts = name.split("[\\-.]");
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
