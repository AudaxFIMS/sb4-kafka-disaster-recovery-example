package dev.semeshin.kafkadr.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Shared utilities for AdminClient operations: probing clusters and provisioning topics.
 */
public final class KafkaAdminHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminHelper.class);
    private static final int DEFAULT_TIMEOUT_MS = 3000;

    /**
     * Mirror of {@code kafka-dr.debug.enable}. Pushed in by {@link DynamicBindingRegistrar}
     * once the properties are bound: this class is a static utility, not a bean, so it has
     * nowhere for Spring to inject into.
     */
    private static volatile boolean debugEnabled = false;

    private KafkaAdminHelper() {}

    public static void setDebugEnabled(boolean enabled) {
        debugEnabled = enabled;
    }

    public static boolean isDebugEnabled() {
        return debugEnabled;
    }

    private static final String KAFKA_BINDER_CONFIG_PREFIX = "spring.cloud.stream.kafka.binder.configuration.";

    public static boolean probeCluster(String brokers, int timeoutMs, Map<String, String> kafkaClientProps) {
        try (AdminClient admin = createAdminClient(brokers, timeoutMs, kafkaClientProps)) {
            admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            if (isDebugEnabled()) {
                log.warn("Probe failed for brokers={}, timeoutMs={}", brokers, timeoutMs, e);
            }
            return false;
        }
    }

    public static boolean probeCluster(String brokers, int timeoutMs) {
        return probeCluster(brokers, timeoutMs, Map.of());
    }

    public static boolean probeCluster(String brokers) {
        return probeCluster(brokers, DEFAULT_TIMEOUT_MS, Map.of());
    }

    /**
     * Probes a cluster using Kafka client properties extracted from the cluster's effective environment.
     *
     * @param clusterId the cluster's binder id
     */
    public static boolean probeCluster(String clusterId, KafkaClusterProperties props) {
        String brokers = props.findCluster(clusterId).getBootstrapServers();
        Map<String, String> kafkaProps = extractKafkaClientProperties(props.getEffectiveEnvironment(clusterId));
        return probeCluster(brokers, DEFAULT_TIMEOUT_MS, kafkaProps);
    }

    /**
     * Creates the topics of the cluster's own group that are missing on it. Topics of other
     * groups belong to a different Kafka and are never created here.
     *
     * @param cluster the cluster's binder id
     */
    public static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props, int timeoutMs) {
        provisionTopics(cluster, brokers, props, props.topology(), timeoutMs);
    }

    /**
     * Same, with the topology already resolved — what the starter itself uses, so a provisioning
     * round does not rebuild the whole topology for every cluster it touches.
     */
    public static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props,
                                       ClusterTopology topology) {
        provisionTopics(cluster, brokers, props, topology, DEFAULT_TIMEOUT_MS);
    }

    private static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props,
                                        ClusterTopology topology, int timeoutMs) {
        ClusterTopology.Group group = topology.groupOfCluster(cluster);
        if (group == null) {
            // Silently creating nothing would leave the topics missing until a failover needs them.
            log.warn("[{}] Not a configured cluster id — no topics provisioned. A cluster of a cluster group is "
                    + "addressed by its binder id <group>-<cluster>; configured: {}", cluster,
                    topology.clusters().stream().map(ClusterTopology.ClusterRef::id).toList());
            return;
        }
        Set<String> requiredTopics = group.topics();

        if (requiredTopics.isEmpty()) return;

        Map<String, String> env = props.getEffectiveEnvironment(cluster);
        Map<String, String> kafkaProps = extractKafkaClientProperties(env);

        // Get replication factor from binder config, default to broker setting
        String rfValue = env.get("spring.cloud.stream.kafka.binder.replication-factor");
        Optional<Short> replicationFactor = (rfValue != null)
                ? Optional.of(Short.parseShort(rfValue))
                : Optional.empty();

        try (AdminClient admin = createAdminClient(brokers, timeoutMs, kafkaProps)) {
            Set<String> existing = admin.listTopics().names().get(timeoutMs, TimeUnit.MILLISECONDS);
            List<NewTopic> toCreate = requiredTopics.stream()
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, Optional.empty(), replicationFactor))
                    .toList();

            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(timeoutMs, TimeUnit.MILLISECONDS);
                log.info("[{}] Created topics: {}", cluster,
                        toCreate.stream().map(NewTopic::name).toList());
            }
        } catch (Exception e) {
            if (debugEnabled) {
                log.warn("[{}] Topic provisioning failed", cluster, e);
            } else {
                log.warn("[{}] Topic provisioning failed: {}", cluster, e.getMessage());
            }
        }
    }

    public static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props) {
        provisionTopics(cluster, brokers, props, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Extracts raw Kafka client properties from the effective environment.
     * Properties under spring.cloud.stream.kafka.binder.configuration.* are Kafka client properties.
     */
    public static Map<String, String> extractKafkaClientProperties(Map<String, String> effectiveEnvironment) {
        Map<String, String> kafkaProps = new HashMap<>();
        for (Map.Entry<String, String> entry : effectiveEnvironment.entrySet()) {
            if (entry.getKey().startsWith(KAFKA_BINDER_CONFIG_PREFIX)) {
                String kafkaKey = entry.getKey().substring(KAFKA_BINDER_CONFIG_PREFIX.length());
                kafkaProps.put(kafkaKey, entry.getValue());
            }
        }
        return kafkaProps;
    }

    public static AdminClient createAdminClient(String brokers, int timeoutMs) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs,
                "socket.connection.setup.timeout.ms", timeoutMs
        ));
    }

    public static AdminClient createAdminClient(String brokers, int timeoutMs, Map<String, String> extraProps) {
        Map<String, Object> config = new HashMap<>(extraProps);
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        // Bound the initial socket connection setup too — otherwise an unreachable
        // host (dropped SYN) falls back to Kafka's 10s default, so describeCluster()
        // and the subsequent close() block far longer than timeoutMs.
        config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, (long) timeoutMs);
        config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, (long) timeoutMs);
		// Override properties from manual settings
	    config.putAll(extraProps);

        return AdminClient.create(config);
    }
}
