package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.LinkedHashMap;
import java.util.Map;

import static dev.semeshin.kafkadr.config.ClusterTopologyTest.cluster;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.clusters;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.consumer;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.group;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.groups;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.producer;
import static dev.semeshin.kafkadr.config.ClusterTopologyTest.twoGroups;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(OutputCaptureExtension.class)
class ClusterTopologyValidatorTest {

    @Test
    void soundTopologyPassesAndIsReturned() {
        ClusterTopology topology = ClusterTopologyValidator.validate(twoGroups());

        assertThat(topology.groups()).hasSize(2);
    }

    @Test
    void failbackAfterThatIsNotATimeOfDayFailsStartup() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("analytics").getFailover().setFailbackAfter("11pm");

        // Otherwise it would surface only when the group tries to fail back, on every election.
        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'11pm'")
                .hasMessageContaining("kafka-dr.cluster-groups.analytics.failover.failback-after");
    }

    @Test
    void failbackAfterOfAnExplicitDefaultGroupNamesItsOwnProperty() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups("default", group(clusters("a", cluster("a:9092", 1)))));
        props.getClusterGroups().get("default").getFailover().setFailbackAfter("11pm");

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.cluster-groups.default.failover.failback-after");
    }

    @Test
    void globalFailbackAfterIsCheckedForTheLegacyForm() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters("primary", cluster("a:9092", 1), "secondary", cluster("b:9092", 2)));
        props.getFailover().setFailbackAfter("25:00");

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.failover.failback-after");

        props.getFailover().setFailbackAfter("23:30");
        ClusterTopologyValidator.validate(props);
    }

    @Test
    void emptyConfigurationIsLeftToTheCaller() {
        assertThat(ClusterTopologyValidator.validate(new KafkaClusterProperties()).isEmpty()).isTrue();
    }

    // --- one physical cluster, one place ----------------------------------------------

    @Test
    void brokerSharedBetweenGroupsIsRejected() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("analytics").getClusters().get("dc2").setBootstrapServers("core-b:9092");

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broker 'core-b:9092'")
                .hasMessageContaining("cluster 'secondary' of group 'core'")
                .hasMessageContaining("cluster 'dc2' of group 'analytics'")
                .hasMessageContaining("groups must fail independently");
    }

    @Test
    void brokerSharedWithinAGroupIsRejected() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusters(clusters(
                "primary", cluster("kafka-a:9092,kafka-b:9092", 1),
                "secondary", cluster("kafka-c:9092,kafka-b:9092", 2)));

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broker 'kafka-b:9092'")
                .hasMessageContaining("a cluster cannot be its own failover target");
    }

    @Test
    void brokerAddressesAreComparedWithoutSchemeCaseOrWhitespace() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("analytics").getClusters().get("dc1")
                .setBootstrapServers(" SASL_SSL://Core-A:9092 ");

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broker 'core-a:9092'");
    }

    @Test
    void normalizedBrokersSplitTrimAndDropTheScheme() {
        assertThat(ClusterTopology.normalizedBrokers("PLAINTEXT://Kafka-A:9092, kafka-b:9093,,"))
                .containsExactly("kafka-a:9092", "kafka-b:9093");
    }

    @Test
    void clusterWithoutBootstrapServersIsRejected() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("core").getClusters().get("secondary").setBootstrapServers(" ");

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cluster 'secondary' of group 'core' has no bootstrap-servers");
    }

    // --- names --------------------------------------------------------------------------

    @Test
    void groupNameThatCannotBecomeAPropertyKeyIsRejected() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups("core.eu", group(clusters("primary", cluster("kafka-a:9092", 1)))));

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cluster group name 'core.eu'");
    }

    @Test
    void clusterNameInAGroupIsCheckedToo() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups("core", group(clusters("-primary", cluster("kafka-a:9092", 1)))));

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cluster name '-primary'")
                .hasMessageContaining("kafka-dr.cluster-groups.core.clusters.-primary");
    }

    @Test
    void distinctNamesThatCamelCaseIntoTheSameBeanAreRejected() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(groups(
                "default", group(clusters("Primary", cluster("kafka-a:9092", 1))),
                "core", group(clusters("primary", cluster("kafka-b:9092", 1)))));
        Map<String, KafkaClusterProperties.ConsumerConfig> consumers = new LinkedHashMap<>();
        consumers.put("orders-core", consumer("orders", "default"));
        consumers.put("orders", consumer("orders", "core"));
        props.setConsumers(consumers);

        // ordersCore + Primary on the default group, orders + Core + Primary on group core.
        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bean name 'ordersCorePrimary'");
    }

    // --- producers ------------------------------------------------------------------------

    @Test
    void sameTopicInDifferentGroupsIsFineForProducers() {
        KafkaClusterProperties props = twoGroups();
        props.setProducers(new LinkedHashMap<>(Map.of(
                "core-events", producer("events", "core"),
                "analytics-events", producer("events", "analytics"))));

        assertThat(ClusterTopologyValidator.validate(props).group("analytics").producers()).hasSize(1);
    }

    @Test
    void secondProducerForATopicInTheSameGroupIsRejected() {
        KafkaClusterProperties props = twoGroups();
        Map<String, KafkaClusterProperties.ProducerConfig> producers = new LinkedHashMap<>();
        producers.put("events", producer("events", "core"));
        producers.put("events-again", producer("events", "core"));
        props.setProducers(producers);

        assertThatThrownBy(() -> ClusterTopologyValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'events' and 'events-again' both write topic 'events' in cluster group 'core'");
    }

    // --- schema registry ------------------------------------------------------------------

    @Test
    void schemaRegistryInBindingPropertiesIsReportedWhenClustersHaveTheirOwn(CapturedOutput output) {
        KafkaClusterProperties props = twoGroups();
        ClusterConfig primary = props.getClusterGroups().get("core").getClusters().get("primary");
        primary.setEnvironment(Map.of("spring.cloud.stream.kafka.binder.configuration.schema.registry.url",
                "http://sr-core-a:8081"));
        KafkaClusterProperties.ConsumerConfig orders = consumer("orders", "core");
        orders.setProperties(Map.of("configuration", Map.of("schema.registry.url", "http://sr-all:8081")));
        props.setConsumers(Map.of("orders", orders));

        ClusterTopologyValidator.validate(props);

        assertThat(output).contains("kafka-dr.consumers.orders.properties sets [configuration.schema.registry.url]")
                .contains("group 'core'")
                .contains("clusters [primary]");
    }

    @Test
    void groupLevelRegistryCountsForEveryClusterOfTheGroup(CapturedOutput output) {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("analytics").setDefaultEnvironment(Map.of(
                "spring.cloud.stream.kafka.binder.configuration.schema.registry.url", "http://sr-an:8081"));
        KafkaClusterProperties.ProducerConfig events = producer("events", "analytics");
        events.setProperties(Map.of("configuration", Map.of("schema.registry.url", "http://sr-all:8081")));
        props.setProducers(Map.of("events", events));

        ClusterTopologyValidator.validate(props);

        assertThat(output).contains("kafka-dr.producers.events.properties sets")
                .contains("clusters [dc1, dc2]");
    }

    @Test
    void schemaRegistryInBindingPropertiesIsQuietWithOnlyAGlobalRegistry(CapturedOutput output) {
        KafkaClusterProperties props = twoGroups();
        props.setDefaultEnvironment(Map.of(
                "spring.cloud.stream.kafka.binder.configuration.schema.registry.url", "http://sr-global:8081"));
        KafkaClusterProperties.ConsumerConfig orders = consumer("orders", "core");
        orders.setProperties(Map.of("configuration", Map.of("schema.registry.url", "http://sr-orders:8081")));
        props.setConsumers(Map.of("orders", orders));

        ClusterTopologyValidator.validate(props);

        assertThat(output).doesNotContain("properties sets");
    }
}
