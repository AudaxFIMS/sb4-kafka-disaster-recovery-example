package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterGroupConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.FailoverConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.HealthCheckConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ProducerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The configured clusters resolved into cluster groups, with every consumer and producer
 * assigned to the group it belongs to.
 *
 * <p>Both configuration forms end up here. The legacy {@code kafka-dr.clusters} map becomes a
 * single group named {@code default} that uses the global settings as they are; each entry of
 * {@code kafka-dr.cluster-groups} becomes a group whose settings are the global ones narrowed
 * by its own overrides. Everything downstream works on groups and binder ids and no longer
 * needs to know which form was used.
 *
 * <p>Only what is needed to build the groups at all is checked here — both forms at once, an
 * empty group, an unknown or missing {@code cluster-group}, colliding binder ids, a second
 * producer for a topic in the same group (which would leave the topic without one route). The checks
 * that make a buildable topology a sensible one live in {@link ClusterTopologyValidator}, which
 * runs once at startup.
 */
public final class ClusterTopology {

    /** A scheme some clients accept in bootstrap.servers, e.g. {@code SASL_SSL://}. */
    private static final Pattern SCHEME = Pattern.compile("^[A-Za-z_]+://");

    /** One cluster of a group, addressed by its binder id. */
    public record ClusterRef(String group, String name, String id, ClusterConfig config) {

        public String bootstrapServers() {
            return config.getBootstrapServers();
        }

        public int priority() {
            return config.getPriority();
        }

        /** Function bean name of a consumer on this cluster. */
        public String functionName(String consumerName) {
            return KafkaClusterProperties.functionName(consumerName, group, name);
        }

        /** Input binding name of a consumer on this cluster. */
        public String bindingName(String consumerName) {
            return KafkaClusterProperties.bindingName(consumerName, group, name);
        }
    }

    /** One logical Kafka: its clusters, effective settings, consumers and producers. */
    public static final class Group {
        private final String name;
        private final List<ClusterRef> clusters;
        private final List<ClusterRef> clustersByPriority;
        private final HealthCheckConfig healthCheck;
        private final FailoverConfig failover;
        private final boolean autoCreateTopics;
        private final List<ConsumerConfig> consumers = new ArrayList<>();
        private final List<ProducerConfig> producers = new ArrayList<>();

        private Group(String name, List<ClusterRef> clusters, HealthCheckConfig healthCheck,
                      FailoverConfig failover, boolean autoCreateTopics) {
            this.name = name;
            this.clusters = List.copyOf(clusters);
            // Stable sort: equal priorities keep their declaration order, as before.
            this.clustersByPriority = clusters.stream()
                    .sorted(Comparator.comparingInt(ClusterRef::priority))
                    .toList();
            this.healthCheck = healthCheck;
            this.failover = failover;
            this.autoCreateTopics = autoCreateTopics;
        }

        public String name() {
            return name;
        }

        public boolean isDefault() {
            return KafkaClusterProperties.DEFAULT_CLUSTER_GROUP.equals(name);
        }

        /** Clusters in declaration order. */
        public List<ClusterRef> clusters() {
            return clusters;
        }

        /** Clusters in failover order — lowest priority value first. */
        public List<ClusterRef> clustersByPriority() {
            return clustersByPriority;
        }

        public HealthCheckConfig healthCheck() {
            return healthCheck;
        }

        public FailoverConfig failover() {
            return failover;
        }

        public boolean autoCreateTopics() {
            return autoCreateTopics;
        }

        public List<ConsumerConfig> consumers() {
            return Collections.unmodifiableList(consumers);
        }

        public List<ProducerConfig> producers() {
            return Collections.unmodifiableList(producers);
        }

        /** Topics this group's consumers and producers use — what its clusters must carry. */
        public Set<String> topics() {
            Set<String> topics = new LinkedHashSet<>();
            consumers.forEach(c -> topics.add(c.getTopic()));
            producers.forEach(p -> topics.add(p.getTopic()));
            topics.remove(null);
            return topics;
        }
    }

    private final Map<String, Group> groups;
    private final Map<String, ClusterRef> clustersById;
    private final Map<String, Group> groupByConsumer;
    private final Map<String, Group> groupByProducer;

    private ClusterTopology(Map<String, Group> groups, Map<String, ClusterRef> clustersById,
                            Map<String, Group> groupByConsumer, Map<String, Group> groupByProducer) {
        this.groups = groups;
        this.clustersById = clustersById;
        this.groupByConsumer = groupByConsumer;
        this.groupByProducer = groupByProducer;
    }

    /**
     * @throws IllegalStateException when the configuration cannot be resolved into groups
     */
    public static ClusterTopology from(KafkaClusterProperties props) {
        Map<String, ClusterGroupConfig> declared = props.getClusterGroups() == null
                ? Map.of() : props.getClusterGroups();
        Map<String, ClusterConfig> legacy = props.getClusters() == null ? Map.of() : props.getClusters();

        if (!legacy.isEmpty() && !declared.isEmpty()) {
            throw new IllegalStateException(
                    ("Both kafka-dr.clusters and kafka-dr.cluster-groups are configured. Use one form: move "
                            + "the clusters under kafka-dr.cluster-groups.%s.clusters to keep their names, or "
                            + "drop cluster-groups.").formatted(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP));
        }

        Map<String, Group> groups = new LinkedHashMap<>();
        if (!legacy.isEmpty()) {
            // The legacy form uses the global settings directly, not copies, so it behaves
            // exactly as it did before groups existed.
            groups.put(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP, new Group(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP,
                    refs(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP, legacy), props.getHealthCheck(), props.getFailover(),
                    props.isAutoCreateTopics()));
        }
        for (Map.Entry<String, ClusterGroupConfig> entry : declared.entrySet()) {
            String name = entry.getKey();
            ClusterGroupConfig cfg = entry.getValue();
            if (cfg == null || cfg.getClusters() == null || cfg.getClusters().isEmpty()) {
                throw new IllegalStateException(
                        "Cluster group '%s' has no clusters. Add kafka-dr.cluster-groups.%s.clusters.<name>.bootstrap-servers"
                                .formatted(name, name));
            }
            groups.put(name, new Group(name, refs(name, cfg.getClusters()),
                    cfg.getHealthCheck() == null ? props.getHealthCheck() : cfg.getHealthCheck().applyTo(props.getHealthCheck()),
                    cfg.getFailover() == null ? props.getFailover() : cfg.getFailover().applyTo(props.getFailover()),
                    cfg.getAutoCreateTopics() == null ? props.isAutoCreateTopics() : cfg.getAutoCreateTopics()));
        }

        Map<String, ClusterRef> clustersById = new LinkedHashMap<>();
        for (Group group : groups.values()) {
            for (ClusterRef ref : group.clusters()) {
                ClusterRef previous = clustersById.put(ref.id(), ref);
                if (previous != null) {
                    throw new IllegalStateException(
                            ("Clusters '%s' of group '%s' and '%s' of group '%s' both resolve to binder id '%s'. "
                                    + "Rename one of them.").formatted(previous.name(), previous.group(),
                                    ref.name(), ref.group(), ref.id()));
                }
            }
        }

        Map<String, Group> groupByConsumer = new LinkedHashMap<>();
        Map<String, Group> groupByProducer = new LinkedHashMap<>();
        if (!groups.isEmpty()) {
            for (ConsumerConfig consumer : props.getConsumers().values()) {
                Group group = assign(groups, "consumers", consumer.getName(), consumer.getClusterGroup());
                group.consumers.add(consumer);
                groupByConsumer.put(consumer.getName(), group);
            }
            for (ProducerConfig producer : props.getProducers().values()) {
                Group group = assign(groups, "producers", producer.getName(), producer.getClusterGroup());
                for (ProducerConfig other : group.producers) {
                    // The same topic in two groups is two Kafkas; within one it has to be one producer.
                    if (Objects.equals(other.getTopic(), producer.getTopic())) {
                        throw new IllegalStateException(
                                ("Producers '%s' and '%s' both write topic '%s' in cluster group '%s'. A topic has "
                                        + "one producer per cluster group.").formatted(other.getName(),
                                        producer.getName(), producer.getTopic(), group.name()));
                    }
                }
                group.producers.add(producer);
                groupByProducer.put(producer.getName(), group);
            }
        }

        return new ClusterTopology(Collections.unmodifiableMap(groups), Collections.unmodifiableMap(clustersById),
                groupByConsumer, groupByProducer);
    }

    private static List<ClusterRef> refs(String group, Map<String, ClusterConfig> clusters) {
        List<ClusterRef> refs = new ArrayList<>(clusters.size());
        clusters.forEach((name, cfg) ->
                refs.add(new ClusterRef(group, name, KafkaClusterProperties.clusterId(group, name), cfg)));
        return refs;
    }

    /**
     * The group a consumer or producer belongs to. Omitting {@code cluster-group} is allowed
     * only while there is a single group to fall back to — with several, guessing would bind
     * it to the wrong Kafka.
     */
    private static Group assign(Map<String, Group> groups, String kind, String name, String clusterGroup) {
        if (clusterGroup == null || clusterGroup.isBlank()) {
            if (groups.size() == 1) {
                return groups.values().iterator().next();
            }
            throw new IllegalStateException(
                    ("kafka-dr.%s.%s has no cluster-group, but %d groups are configured %s. Set "
                            + "kafka-dr.%s.%s.cluster-group.").formatted(kind, name, groups.size(),
                            groups.keySet(), kind, name));
        }
        Group group = groups.get(clusterGroup.trim());
        if (group == null) {
            throw new IllegalStateException(
                    "kafka-dr.%s.%s refers to cluster-group '%s', which is not configured. Known groups: %s"
                            .formatted(kind, name, clusterGroup, groups.keySet()));
        }
        return group;
    }

    // --- queries --------------------------------------------------------------

    /** True when no cluster is configured in either form. */
    public boolean isEmpty() {
        return groups.isEmpty();
    }

    public Collection<Group> groups() {
        return groups.values();
    }

    /** @throws IllegalArgumentException for an unknown group */
    public Group group(String name) {
        Group group = groups.get(name);
        if (group == null) {
            throw new IllegalArgumentException("Unknown cluster group '%s'. Known groups: %s"
                    .formatted(name, groups.keySet()));
        }
        return group;
    }

    public boolean isMultiGroup() {
        return groups.size() > 1;
    }

    /** Every cluster of every group, groups in declaration order. */
    public List<ClusterRef> clusters() {
        return List.copyOf(clustersById.values());
    }

    /** The cluster with this binder id, or null. */
    public ClusterRef findCluster(String clusterId) {
        return clustersById.get(clusterId);
    }

    /**
     * Broker addresses of a {@code bootstrap.servers} value, comparable across clusters:
     * split on commas, trimmed, lower-cased, without a {@code SASL_SSL://}-style scheme.
     */
    public static Set<String> normalizedBrokers(String bootstrapServers) {
        Set<String> brokers = new LinkedHashSet<>();
        for (String raw : bootstrapServers.split(",")) {
            String broker = SCHEME.matcher(raw.trim()).replaceFirst("").toLowerCase(Locale.ROOT);
            if (!broker.isEmpty()) {
                brokers.add(broker);
            }
        }
        return brokers;
    }

    /** The group of the cluster with this binder id, or null. */
    public Group groupOfCluster(String clusterId) {
        ClusterRef ref = clustersById.get(clusterId);
        return ref == null ? null : groups.get(ref.group());
    }

    /** The group a consumer reads from, or null when no cluster is configured. */
    public Group groupOf(ConsumerConfig consumer) {
        return groupByConsumer.get(consumer.getName());
    }

    /** The group a producer writes to, or null when no cluster is configured. */
    public Group groupOf(ProducerConfig producer) {
        return groupByProducer.get(producer.getName());
    }
}
