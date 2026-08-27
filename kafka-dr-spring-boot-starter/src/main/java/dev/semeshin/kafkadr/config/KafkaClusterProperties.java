package dev.semeshin.kafkadr.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
@ConfigurationProperties(prefix = "kafka-dr")
public class KafkaClusterProperties {

    private Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
    private Map<String, ConsumerConfig> consumers = new LinkedHashMap<>();
    private Map<String, ProducerConfig> producers = new LinkedHashMap<>();
    private Map<String, Object> defaultEnvironment = new LinkedHashMap<>();
    private Map<String, Object> defaultConsumerProperties = new LinkedHashMap<>();
    private Map<String, Object> defaultProducerProperties = new LinkedHashMap<>();
    private boolean autoCreateTopics = false;
    private FailoverConfig failover = new FailoverConfig();
    private HealthCheckConfig healthCheck = new HealthCheckConfig();
    private IdempotencyConfig idempotency = new IdempotencyConfig();
    private DebugConfig debug = new DebugConfig();

    public Map<String, ClusterConfig> getClusters() { return clusters; }
    public void setClusters(Map<String, ClusterConfig> clusters) { this.clusters = clusters; }

    /**
     * Returns the consumer map with each entry's {@code name} populated from its map key.
     * The map key acts as the logical consumer identifier — used for bean naming,
     * binding naming, idempotency scoping, and timestamp tracking.
     */
    public Map<String, ConsumerConfig> getConsumers() {
        consumers.forEach((name, cfg) -> {
            if (cfg.getName() == null) cfg.setName(name);
        });
        return consumers;
    }

    public void setConsumers(Map<String, ConsumerConfig> consumers) { this.consumers = consumers; }

    /**
     * Returns the producer map with each entry's {@code name} populated from its map key.
     */
    public Map<String, ProducerConfig> getProducers() {
        producers.forEach((name, cfg) -> {
            if (cfg.getName() == null) cfg.setName(name);
        });
        return producers;
    }

    public void setProducers(Map<String, ProducerConfig> producers) { this.producers = producers; }

    public Map<String, Object> getDefaultEnvironment() { return defaultEnvironment; }
    public void setDefaultEnvironment(Map<String, Object> defaultEnvironment) { this.defaultEnvironment = defaultEnvironment; }
    public Map<String, Object> getDefaultConsumerProperties() { return defaultConsumerProperties; }
    public void setDefaultConsumerProperties(Map<String, Object> defaultConsumerProperties) { this.defaultConsumerProperties = defaultConsumerProperties; }
    public Map<String, Object> getDefaultProducerProperties() { return defaultProducerProperties; }
    public void setDefaultProducerProperties(Map<String, Object> defaultProducerProperties) { this.defaultProducerProperties = defaultProducerProperties; }
    public boolean isAutoCreateTopics() { return autoCreateTopics; }
    public void setAutoCreateTopics(boolean autoCreateTopics) { this.autoCreateTopics = autoCreateTopics; }
    public FailoverConfig getFailover() { return failover; }
    public void setFailover(FailoverConfig failover) { this.failover = failover; }
    public HealthCheckConfig getHealthCheck() { return healthCheck; }
    public void setHealthCheck(HealthCheckConfig healthCheck) { this.healthCheck = healthCheck; }
    public IdempotencyConfig getIdempotency() { return idempotency; }
    public void setIdempotency(IdempotencyConfig idempotency) { this.idempotency = idempotency; }
    public DebugConfig getDebug() { return debug; }
    public void setDebug(DebugConfig debug) { this.debug = debug; }

    public boolean isIdempotencyEnabled() { return idempotency.isEnabled(); }

    /**
     * Effective idempotency setting for one consumer: the global flag, optionally
     * narrowed by the consumer's own override.
     *
     * <p>The override can only turn deduplication off. Turning it on for a single
     * consumer while it is globally disabled is not possible, because the
     * IdempotencyStore bean itself is conditional on the global flag.
     */
    public boolean isIdempotencyEnabled(ConsumerConfig consumer) {
        if (!idempotency.isEnabled()) {
            return false;
        }
        Boolean override = consumer.getIdempotencyEnabled();
        return override == null || override;
    }

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
        /**
         * Populated from the consumer map key. Used as logical identifier for
         * bean naming, idempotency scoping, and timestamp tracking.
         */
        private String name;

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
         * Batch consumption settings. Disabled by default — each record is delivered
         * to the handler on its own, as before.
         */
        private BatchConfig batch = new BatchConfig();

        /**
         * Per-consumer override of kafka-dr.idempotency.enabled. Null means "follow the
         * global flag". Can only narrow it: the IdempotencyStore bean is created based on
         * the global flag, so a consumer cannot switch deduplication on when it is off
         * application-wide.
         */
        private Boolean idempotencyEnabled;

        /**
         * Per-consumer properties, merged on top of kafka-dr.default-consumer-properties.
         * Keys are routed by {@link BindingPropertyRouter} into either
         * spring.cloud.stream.bindings.{binding}.consumer.* (core: concurrency,
         * max-attempts, back-off-*) or
         * spring.cloud.stream.kafka.bindings.{binding}.consumer.* (Kafka extension:
         * ack-mode, enable-dlq, configuration.*).
         */
        private Map<String, Object> properties = new LinkedHashMap<>();

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getGroup() { return group; }
        public void setGroup(String group) { this.group = group; }
        public String getHandler() { return handler; }
        public void setHandler(String handler) { this.handler = handler; }
        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public BatchConfig getBatch() { return batch; }
        public void setBatch(BatchConfig batch) { this.batch = batch; }
        public Boolean getIdempotencyEnabled() { return idempotencyEnabled; }
        public void setIdempotencyEnabled(Boolean idempotencyEnabled) { this.idempotencyEnabled = idempotencyEnabled; }
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
    }

    /**
     * Batch consumption for a single consumer. The starter owns the resulting
     * {@code batch-mode} binding property, because the flag also decides which
     * consumer function bean is registered.
     */
    public static class BatchConfig {

        /**
         * How the batch reaches the handler.
         * <ul>
         *   <li>{@code SPLIT} (default) — the starter unpacks the batch envelope back into
         *       per-record messages, so idempotency, watermarks and existing
         *       {@code Message<T>} handlers keep working unchanged.</li>
         *   <li>{@code STANDARD} — the handler receives the raw {@code Message<List<T>>}
         *       envelope, exactly as plain Spring Cloud Stream delivers it. Per-record
         *       deduplication is not possible in this mode.</li>
         * </ul>
         */
        public enum Mode { SPLIT, STANDARD }

        /** What happens to the rest of the batch when one record fails. */
        public enum ErrorPolicy {
            /** Rethrow so the container commits the prefix and redelivers from the failure. */
            FAIL_BATCH,
            /** Log and continue with the remaining records. Breaks per-key ordering. */
            SKIP_FAILED
        }

        private boolean enabled = false;
        private Mode mode = Mode.SPLIT;

        /**
         * Maps to max.poll.records. Raise max.poll.interval.ms alongside it:
         * this many records times the per-record processing time must fit inside it,
         * or the consumer is evicted from the group mid-batch.
         */
        private Integer maxRecords;

        /** Maps to fetch.min.bytes. */
        private Integer minBytes;

        /** Maps to fetch.max.wait.ms. */
        private Long maxWaitMs;

        private ErrorPolicy errorPolicy = ErrorPolicy.FAIL_BATCH;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Mode getMode() { return mode; }
        public void setMode(Mode mode) { this.mode = mode; }
        public Integer getMaxRecords() { return maxRecords; }
        public void setMaxRecords(Integer maxRecords) { this.maxRecords = maxRecords; }
        public Integer getMinBytes() { return minBytes; }
        public void setMinBytes(Integer minBytes) { this.minBytes = minBytes; }
        public Long getMaxWaitMs() { return maxWaitMs; }
        public void setMaxWaitMs(Long maxWaitMs) { this.maxWaitMs = maxWaitMs; }
        public ErrorPolicy getErrorPolicy() { return errorPolicy; }
        public void setErrorPolicy(ErrorPolicy errorPolicy) { this.errorPolicy = errorPolicy; }
    }

    /**
     * Defines a producer for a specific topic.
     * Output binding properties are generated for each producer x cluster pair.
     */
    public static class ProducerConfig {
        /**
         * Populated from the producer map key. Used as logical identifier for binding naming.
         */
        private String name;

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

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
    }

    public static class FailoverConfig {
        /**
         * When true and cross-cluster replication is active, consumers on the new cluster
         * seek to the offset matching the timestamp of the last processed message,
         * skipping already-processed replicated data. Idempotency store provides
         * additional deduplication for messages in the boundary window.
         */
        private boolean seekByTimestamp = false;

        /**
         * Time of day (HH:mm:ss) after which failback to a higher-priority cluster is allowed.
         * When set, after a failover the app stays on the current cluster until this time.
         * Null or empty = immediate failback (default).
         * Example: "23:59:59" = failback only after midnight maintenance window.
         */
        private String failbackAfter;

        public boolean isSeekByTimestamp() { return seekByTimestamp; }
        public void setSeekByTimestamp(boolean seekByTimestamp) { this.seekByTimestamp = seekByTimestamp; }
        public String getFailbackAfter() { return failbackAfter; }
        public void setFailbackAfter(String failbackAfter) { this.failbackAfter = failbackAfter; }
    }

    public static class HealthCheckConfig {
        private long intervalMs = 5000;
        private long timeoutMs = 2000;
        private int failureThreshold = 2;
        private int recoveryThreshold = 3;

        /**
         * When true, health check verifies partition leader availability via
         * describeTopics() in addition to describeCluster(). Catches scenarios
         * where cluster metadata is reachable but brokers can't serve data
         * (no partition leaders). No data is written — read-only check.
         */
        private boolean deepProbe = false;

        /**
         * Minimum number of active nodes (unique partition leaders) required
         * for the cluster to be considered healthy during deep probe.
         * Default: 1 — at least one active leader must exist.
         */
        private int deepProbeMinNodes = 1;

        /**
         * Minimum number of in-sync replicas (ISR) per partition required
         * for the cluster to be considered healthy during deep probe.
         * Default: 0 — ISR check disabled (only leader presence is verified).
         */
        private int deepProbeMinIsr = 0;

        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public long getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
        public int getRecoveryThreshold() { return recoveryThreshold; }
        public void setRecoveryThreshold(int recoveryThreshold) { this.recoveryThreshold = recoveryThreshold; }
        public boolean isDeepProbe() { return deepProbe; }
        public void setDeepProbe(boolean deepProbe) { this.deepProbe = deepProbe; }
        public int getDeepProbeMinNodes() { return deepProbeMinNodes; }
        public void setDeepProbeMinNodes(int deepProbeMinNodes) { this.deepProbeMinNodes = deepProbeMinNodes; }
        public int getDeepProbeMinIsr() { return deepProbeMinIsr; }
        public void setDeepProbeMinIsr(int deepProbeMinIsr) { this.deepProbeMinIsr = deepProbeMinIsr; }
    }

    public static class DebugConfig {
        /**
         * Turns on the diagnostic logging the starter otherwise keeps quiet, starting with
         * the reason an AdminClient probe failed. Off by default: a probe failure is the
         * normal signal during a failover, and logging every stack trace would bury it.
         */
        private boolean enable = false;

        public boolean isEnable() { return enable; }
        public void setEnable(boolean enable) { this.enable = enable; }
    }

    public static class IdempotencyConfig {
        /**
         * Master switch for the idempotency mechanism. Enabled by default;
         * set kafka-dr.idempotency.enabled=false to skip deduplication entirely —
         * no IdempotencyStore bean is used and consumers process every message.
         */
        private boolean enabled = true;

        private long ttlSeconds = 3600;
        private String keyPrefix = "idempotency";
        private String keyHeader;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
        public String getKeyPrefix() { return keyPrefix; }
        public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
        public String getKeyHeader() { return keyHeader; }
        public void setKeyHeader(String keyHeader) { this.keyHeader = keyHeader; }
    }

    /**
     * Acknowledgment mode configured for a consumer, or null when none was set and the
     * container default (BATCH) applies.
     *
     * <p>Read from the same {@code properties} map the binder gets it from, so there is one
     * source of truth. The starter needs it because in batch mode it owns the commit: it
     * decides how far the batch may be acknowledged and only then advances the watermark.
     */
    public ContainerProperties.AckMode resolveAckMode(ConsumerConfig consumer) {
        for (Map.Entry<String, String> entry : getEffectiveConsumerProperties(consumer).entrySet()) {
            if (!"ackmode".equals(entry.getKey().replaceAll("[^A-Za-z0-9]", "").toLowerCase())) {
                continue;
            }
            String value = entry.getValue();
            if (value == null || value.isBlank()) {
                return null;
            }
            try {
                return ContainerProperties.AckMode.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(
                        "Consumer '%s' has an unknown ack-mode '%s'".formatted(consumer.getName(), value), e);
            }
        }
        return null;
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

    /**
     * Spring Cloud Function bean name for a (consumer name, cluster) pair.
     * Input is the logical consumer name (Map key), not the topic.
     */
    public static String functionName(String consumerName, String cluster) {
        return toCamelCase(consumerName) + capitalize(cluster);
    }

    /**
     * Input binding name for a (consumer name, cluster) pair.
     */
    public static String bindingName(String consumerName, String cluster) {
        return functionName(consumerName, cluster) + "-in-0";
    }

    /**
     * Output binding name for a producer.
     * Input is the logical producer name (Map key), not the topic, so dotted topic names
     * (e.g. "ax123.test.event") don't pollute binding keys.
     */
    public static String producerBindingName(String producerName) {
        return toCamelCase(producerName);
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
