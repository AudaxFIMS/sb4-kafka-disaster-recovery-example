package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.ClusterTopology.ClusterRef;
import dev.semeshin.kafkadr.config.ClusterTopology.Group;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterGroupConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Checks the cluster topology at startup.
 *
 * <p>{@link ClusterTopology#from} already refuses what cannot be built. This adds the checks
 * that make a buildable topology a sound one — above all that a physical cluster belongs to
 * exactly one place: two entries pointing at the same brokers are either a failover target
 * that is not a failover target at all, or two groups that are not independent, and either
 * way the mistake only shows when a failover moves traffic it was never meant to move.
 *
 * <p>Like {@link ConsumerConfigValidator}, what would lose data or misroute it is rejected;
 * what is merely risky is logged.
 */
final class ClusterTopologyValidator {

    private static final Logger log = LoggerFactory.getLogger(ClusterTopologyValidator.class);

    /**
     * Group and cluster names end up in binder names, property keys and bean names. A dot
     * would split a property key, and anything outside this set has no camel-case form.
     */
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private ClusterTopologyValidator() {
    }

    /**
     * @return the topology, resolved and checked
     * @throws IllegalStateException when the configuration would misroute traffic
     */
    static ClusterTopology validate(KafkaClusterProperties props) {
        checkNames(props);
        ClusterTopology topology = ClusterTopology.from(props);
        if (topology.isEmpty()) {
            return topology;
        }
        checkBootstrapServers(topology);
        checkBrokersNotShared(topology);
        checkFunctionNamesUnique(topology);
        checkFailbackAfter(props, topology);
        warnSchemaRegistryOverrides(props, topology);
        return topology;
    }

    /** Only the explicit form is checked: legacy cluster names have always been accepted. */
    private static void checkNames(KafkaClusterProperties props) {
        if (props.getClusterGroups() == null) {
            return;
        }
        for (Map.Entry<String, ClusterGroupConfig> group : props.getClusterGroups().entrySet()) {
            requireName("Cluster group", group.getKey(), "kafka-dr.cluster-groups." + group.getKey());
            if (group.getValue() == null || group.getValue().getClusters() == null) {
                continue;
            }
            for (String cluster : group.getValue().getClusters().keySet()) {
                requireName("Cluster", cluster, "kafka-dr.cluster-groups.%s.clusters.%s".formatted(group.getKey(), cluster));
            }
        }
    }

    private static void requireName(String kind, String name, String path) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalStateException(
                    ("%s name '%s' (%s) may contain only letters, digits, '-' and '_', and must start with a "
                            + "letter or digit: it becomes part of binder names, property keys and bean names.")
                            .formatted(kind, name, path));
        }
    }

    /**
     * {@code failback-after} is parsed only when a failback is due — a typo would pass startup,
     * let the group fail over, and then make every election of the group throw.
     */
    private static void checkFailbackAfter(KafkaClusterProperties props, ClusterTopology topology) {
        for (Group group : topology.groups()) {
            String failbackAfter = group.failover().getFailbackAfter();
            if (failbackAfter == null || failbackAfter.isBlank()) {
                continue;
            }
            try {
                LocalTime.parse(failbackAfter);
            } catch (DateTimeParseException e) {
                // Declared under cluster-groups — even one named "default" — or the legacy form.
                boolean declared = props.getClusterGroups() != null
                        && props.getClusterGroups().containsKey(group.name());
                String path = !declared
                        ? "kafka-dr.failover.failback-after"
                        : "kafka-dr.cluster-groups.%s.failover.failback-after (or kafka-dr.failover.failback-after)"
                                .formatted(group.name());
                throw new IllegalStateException(
                        "failback-after '%s' of group '%s' (%s) is not a time of day such as \"02:00\" or \"23:30:00\""
                                .formatted(failbackAfter, group.name(), path), e);
            }
        }
    }

    private static void checkBootstrapServers(ClusterTopology topology) {
        for (ClusterRef ref : topology.clusters()) {
            if (ref.bootstrapServers() == null || ref.bootstrapServers().isBlank()) {
                throw new IllegalStateException("Cluster '%s' of group '%s' has no bootstrap-servers"
                        .formatted(ref.name(), ref.group()));
            }
        }
    }

    /**
     * A broker address may appear under one cluster only. Across groups that would make two
     * supposedly independent Kafkas share a failure; within a group it would make a cluster its
     * own standby. Addresses are compared without scheme, case or surrounding whitespace — see
     * {@link ClusterTopology#normalizedBrokers} — so a DNS alias of the same broker is not caught
     * here.
     */
    private static void checkBrokersNotShared(ClusterTopology topology) {
        Map<String, ClusterRef> ownerByBroker = new HashMap<>();
        for (ClusterRef ref : topology.clusters()) {
            for (String broker : ClusterTopology.normalizedBrokers(ref.bootstrapServers())) {
                ClusterRef owner = ownerByBroker.putIfAbsent(broker, ref);
                if (owner != null && owner != ref) {
                    throw new IllegalStateException(
                            ("Broker '%s' is listed by cluster '%s' of group '%s' and by cluster '%s' of group '%s'. "
                                    + "A physical cluster can belong to one cluster entry only: %s.")
                                    .formatted(broker, owner.name(), owner.group(), ref.name(), ref.group(),
                                            owner.group().equals(ref.group())
                                                    ? "a cluster cannot be its own failover target"
                                                    : "groups must fail independently"));
                }
            }
        }
    }

    /**
     * Consumer, group and cluster names are camel-cased into bean names, so distinct names can
     * still meet: consumer {@code orders-core} on cluster {@code Primary} of the default group
     * ({@code ordersCore} + {@code Primary}) and consumer {@code orders} on cluster {@code primary}
     * of group {@code core} ({@code orders} + {@code Core} + {@code Primary}) are both
     * {@code ordersCorePrimary}, although their binder ids differ.
     */
    private static void checkFunctionNamesUnique(ClusterTopology topology) {
        Map<String, String> owners = new LinkedHashMap<>();
        for (Group group : topology.groups()) {
            for (ConsumerConfig consumer : group.consumers()) {
                for (ClusterRef ref : group.clusters()) {
                    String functionName = ref.functionName(consumer.getName());
                    String owner = "consumer '%s' on cluster '%s' of group '%s'"
                            .formatted(consumer.getName(), ref.name(), ref.group());
                    String previous = owners.putIfAbsent(functionName, owner);
                    if (previous != null) {
                        throw new IllegalStateException(
                                "Bean name '%s' would be generated for both %s and %s. Rename a consumer, group or cluster."
                                        .formatted(functionName, previous, owner));
                    }
                }
            }
        }
    }

    /**
     * A {@code schema.registry.*} key under a consumer's or producer's {@code properties}
     * lands on its binding — on every cluster of the group — and so replaces the registry
     * each cluster was given in its environment. That quietly sends every cluster to one
     * registry, which is only noticed when schema ids stop resolving after a failover.
     */
    private static void warnSchemaRegistryOverrides(KafkaClusterProperties props, ClusterTopology topology) {
        for (Group group : topology.groups()) {
            List<String> clustersWithOwnRegistry = clustersWithOwnRegistry(props, group);
            if (clustersWithOwnRegistry.isEmpty()) {
                continue;
            }
            for (ConsumerConfig consumer : group.consumers()) {
                warnIfOverridden("consumers", consumer.getName(), props.getEffectiveConsumerProperties(consumer),
                        group, clustersWithOwnRegistry);
            }
            for (ProducerConfig producer : group.producers()) {
                warnIfOverridden("producers", producer.getName(), props.getEffectiveProducerProperties(producer),
                        group, clustersWithOwnRegistry);
            }
        }
    }

    /** Clusters whose registry is set at group or cluster level, not inherited from the global one. */
    private static List<String> clustersWithOwnRegistry(KafkaClusterProperties props, Group group) {
        ClusterGroupConfig groupConfig = props.getClusterGroups() == null
                ? null : props.getClusterGroups().get(group.name());
        boolean groupLevel = groupConfig != null && definesRegistry(groupConfig.getDefaultEnvironment());
        return group.clusters().stream()
                .filter(ref -> groupLevel || definesRegistry(ref.config().getEnvironment()))
                .map(ClusterRef::name)
                .toList();
    }

    private static boolean definesRegistry(Map<String, Object> environment) {
        Map<String, String> flat = new LinkedHashMap<>();
        KafkaClusterProperties.flatten("", environment, flat);
        return flat.keySet().stream().anyMatch(key -> key.endsWith(SCHEMA_REGISTRY_URL));
    }

    private static void warnIfOverridden(String kind, String name, Map<String, String> properties,
                                         Group group, List<String> clustersWithOwnRegistry) {
        List<String> keys = properties.keySet().stream()
                .filter(key -> key.contains("schema.registry."))
                .toList();
        if (keys.isEmpty()) {
            return;
        }
        log.warn("[{}] kafka-dr.{}.{}.properties sets {} — binding properties apply on every cluster of group '{}', "
                        + "so this replaces the registry configured for clusters {}. Move the setting into the "
                        + "cluster environment unless one registry for all of them is intended.",
                name, kind, name, keys, group.name(), clustersWithOwnRegistry);
    }
}
