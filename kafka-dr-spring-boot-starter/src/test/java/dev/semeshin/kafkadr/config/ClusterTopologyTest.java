package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.ClusterTopology.ClusterRef;
import dev.semeshin.kafkadr.config.ClusterTopology.Group;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterGroupConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTopologyTest {

    // --- legacy form ------------------------------------------------------------

    @Test
    void legacyClustersBecomeTheDefaultGroupWithUnchangedIds() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters("secondary", cluster("kafka-b:9092", 2), "primary", cluster("kafka-a:9092", 1)));

        ClusterTopology topology = props.topology();

        Group group = topology.groups().iterator().next();
        assertThat(group.name()).isEqualTo("default");
        assertThat(group.isDefault()).isTrue();
        assertThat(group.clusters()).extracting(ClusterRef::id).containsExactly("secondary", "primary");
        assertThat(group.clustersByPriority()).extracting(ClusterRef::id).containsExactly("primary", "secondary");
        assertThat(topology.isMultiGroup()).isFalse();
    }

    @Test
    void legacyGroupUsesTheGlobalSettingsThemselves() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters("primary", cluster("kafka-a:9092", 1)));
        props.setAutoCreateTopics(true);

        Group group = props.topology().groups().iterator().next();

        assertThat(group.healthCheck()).isSameAs(props.getHealthCheck());
        assertThat(group.failover()).isSameAs(props.getFailover());
        assertThat(group.autoCreateTopics()).isTrue();
    }

    @Test
    void consumersAndProducersWithoutClusterGroupJoinTheOnlyGroup() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters("primary", cluster("kafka-a:9092", 1)));
        props.setConsumers(Map.of("orders", consumer("orders", null)));
        props.setProducers(Map.of("events", producer("events", null)));

        ClusterTopology topology = props.topology();

        Group group = topology.groups().iterator().next();
        assertThat(topology.groupOf(props.getConsumers().get("orders"))).isSameAs(group);
        assertThat(topology.groupOf(props.getProducers().get("events"))).isSameAs(group);
        assertThat(group.topics()).containsExactlyInAnyOrder("orders", "events");
    }

    @Test
    void noClustersYieldsAnEmptyTopology() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setConsumers(Map.of("orders", consumer("orders", null)));

        ClusterTopology topology = props.topology();

        assertThat(topology.isEmpty()).isTrue();
        assertThat(topology.clusters()).isEmpty();
        assertThat(topology.groupOf(props.getConsumers().get("orders"))).isNull();
    }

    // --- explicit groups ----------------------------------------------------------

    @Test
    void explicitGroupsQualifyBinderIdsWithTheGroupName() {
        KafkaClusterProperties props = twoGroups();

        ClusterTopology topology = props.topology();

        assertThat(topology.isMultiGroup()).isTrue();
        assertThat(topology.clusters()).extracting(ClusterRef::id)
                .containsExactly("core-primary", "core-secondary", "analytics-dc1", "analytics-dc2");
        assertThat(topology.findCluster("analytics-dc1").name()).isEqualTo("dc1");
        assertThat(topology.groupOfCluster("core-secondary").name()).isEqualTo("core");
        assertThat(topology.findCluster("primary")).isNull();
    }

    @Test
    void explicitGroupNamedDefaultKeepsTheLegacyNames() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups("default", group(clusters("primary", cluster("kafka-a:9092", 1)))));
        props.setConsumers(Map.of("orders", consumer("orders", null)));

        ClusterRef ref = props.topology().groups().iterator().next().clusters().get(0);

        assertThat(ref.id()).isEqualTo("primary");
        assertThat(ref.functionName("orders")).isEqualTo("ordersPrimary");
    }

    @Test
    void consumersAndProducersAreAssignedToTheirOwnGroup() {
        KafkaClusterProperties props = twoGroups();
        props.setConsumers(Map.of(
                "orders", consumer("orders", "core"),
                "scores", consumer("scores", "analytics")));
        props.setProducers(Map.of("order-analytics", producer("order-analytics", "analytics")));

        ClusterTopology topology = props.topology();

        assertThat(topology.group("core").consumers()).extracting(ConsumerConfig::getName).containsExactly("orders");
        assertThat(topology.group("analytics").topics()).containsExactlyInAnyOrder("scores", "order-analytics");
        assertThat(topology.group("core").topics()).containsExactly("orders");
    }

    @Test
    void groupOverridesNarrowTheGlobalSettingsAndUnsetFieldsInheritThem() {
        KafkaClusterProperties props = twoGroups();
        props.getHealthCheck().setTimeoutMs(2000);
        props.getHealthCheck().setFailureThreshold(2);
        props.getFailover().setSeekByTimestamp(true);
        props.getFailover().setFailbackAfter("23:00:00");
        props.setAutoCreateTopics(false);

        ClusterGroupConfig analytics = props.getClusterGroups().get("analytics");
        analytics.getHealthCheck().setFailureThreshold(5);
        analytics.getHealthCheck().setDeepProbe(true);
        analytics.getFailover().setFailbackAfter("");
        analytics.setAutoCreateTopics(true);

        ClusterTopology topology = props.topology();
        Group core = topology.group("core");
        Group group = topology.group("analytics");

        assertThat(group.healthCheck().getFailureThreshold()).isEqualTo(5);
        assertThat(group.healthCheck().isDeepProbe()).isTrue();
        assertThat(group.healthCheck().getTimeoutMs()).isEqualTo(2000);
        // An empty value switches the global failback window off for this group only.
        assertThat(group.failover().getFailbackAfter()).isEmpty();
        assertThat(group.failover().isSeekByTimestamp()).isTrue();
        assertThat(group.autoCreateTopics()).isTrue();

        assertThat(core.healthCheck().getFailureThreshold()).isEqualTo(2);
        assertThat(core.healthCheck().isDeepProbe()).isFalse();
        assertThat(core.failover().getFailbackAfter()).isEqualTo("23:00:00");
        assertThat(core.autoCreateTopics()).isFalse();
        // Overrides are copies: the global settings are not touched.
        assertThat(props.getHealthCheck().getFailureThreshold()).isEqualTo(2);
    }

    // --- refusals -------------------------------------------------------------------

    @Test
    void bothConfigurationFormsAtOnceAreRejected() {
        KafkaClusterProperties props = twoGroups();
        props.setClusters(clusters("primary", cluster("kafka-x:9092", 1)));

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Both kafka-dr.clusters and kafka-dr.cluster-groups");
    }

    @Test
    void groupWithoutClustersIsRejected() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().put("empty", new ClusterGroupConfig());

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cluster group 'empty' has no clusters");
    }

    @Test
    void missingClusterGroupIsRejectedWhenThereIsMoreThanOneGroup() {
        KafkaClusterProperties props = twoGroups();
        props.setConsumers(Map.of("orders", consumer("orders", null)));

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.consumers.orders has no cluster-group")
                .hasMessageContaining("[core, analytics]");
    }

    @Test
    void unknownClusterGroupIsRejected() {
        KafkaClusterProperties props = twoGroups();
        props.setProducers(Map.of("events", producer("events", "billing")));

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.producers.events refers to cluster-group 'billing'");
    }

    @Test
    void unknownClusterGroupIsRejectedForTheLegacyFormToo() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters("primary", cluster("kafka-a:9092", 1)));
        props.setConsumers(Map.of("orders", consumer("orders", "core")));

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("refers to cluster-group 'core'")
                .hasMessageContaining("[default]");
    }

    @Test
    void collidingBinderIdsAreRejected() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups(
                "default", group(clusters("core-primary", cluster("kafka-a:9092", 1))),
                "core", group(clusters("primary", cluster("kafka-b:9092", 1)))));

        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("binder id 'core-primary'");
    }

    @Test
    void secondProducerForATopicInOneGroupIsRejectedWhereTheTopologyIsBuilt() {
        KafkaClusterProperties props = twoGroups();
        Map<String, ProducerConfig> producers = new LinkedHashMap<>();
        producers.put("events", producer("events", "core"));
        producers.put("analytics-events", producer("events", "analytics"));
        producers.put("events-again", producer("events", "core"));
        props.setProducers(producers);

        // The one place the rule lives: every component builds its routes from this topology.
        assertThatThrownBy(props::topology)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'events' and 'events-again' both write topic 'events' in cluster group 'core'");
    }

    // --- fixtures ---------------------------------------------------------------------

    static KafkaClusterProperties twoGroups() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups(
                "core", group(clusters(
                        "primary", cluster("core-a:9092", 1),
                        "secondary", cluster("core-b:9092", 2))),
                "analytics", group(clusters(
                        "dc1", cluster("an-a:9092", 1),
                        "dc2", cluster("an-b:9092", 2)))));
        return props;
    }

    static ClusterConfig cluster(String brokers, int priority) {
        ClusterConfig cfg = new ClusterConfig();
        cfg.setBootstrapServers(brokers);
        cfg.setPriority(priority);
        return cfg;
    }

    static Map<String, ClusterConfig> clusters(Object... kv) {
        Map<String, ClusterConfig> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put((String) kv[i], (ClusterConfig) kv[i + 1]);
        }
        return map;
    }

    static ClusterGroupConfig group(Map<String, ClusterConfig> clusters) {
        ClusterGroupConfig cfg = new ClusterGroupConfig();
        cfg.setClusters(clusters);
        return cfg;
    }

    static Map<String, ClusterGroupConfig> groups(Object... kv) {
        Map<String, ClusterGroupConfig> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put((String) kv[i], (ClusterGroupConfig) kv[i + 1]);
        }
        return map;
    }

    static ConsumerConfig consumer(String topic, String clusterGroup) {
        ConsumerConfig c = new ConsumerConfig();
        c.setTopic(topic);
        c.setHandler("handle");
        c.setClusterGroup(clusterGroup);
        return c;
    }

    static ProducerConfig producer(String topic, String clusterGroup) {
        ProducerConfig p = new ProducerConfig();
        p.setTopic(topic);
        p.setClusterGroup(clusterGroup);
        return p;
    }
}
