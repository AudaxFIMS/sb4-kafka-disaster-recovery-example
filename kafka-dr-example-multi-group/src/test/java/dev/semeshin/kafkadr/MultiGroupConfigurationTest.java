package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.AckPolicy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the shipped {@code application.yml} really produces the arrangement this example
 * is meant to demonstrate — two independent groups, bridged both ways with {@code depends-on} —
 * and stays a configuration the starter accepts (its startup checks run in
 * {@code config.MultiGroupStartupValidationTest}).
 */
class MultiGroupConfigurationTest {

    private static KafkaClusterProperties properties;
    private static ClusterTopology topology;

    @BeforeAll
    static void loadShippedConfiguration() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        sources.forEach(environment.getPropertySources()::addFirst);

        properties = Binder.get(environment).bind("kafka-dr", KafkaClusterProperties.class).get();
        topology = properties.topology();
    }

    @Test
    void twoGroupsWithTwoClustersEach() {
        assertThat(topology.groups()).extracting(ClusterTopology.Group::name).containsExactly("core", "analytics");
        assertThat(topology.clusters()).extracting(ClusterTopology.ClusterRef::id)
                .containsExactly("core-a", "core-b", "analytics-a", "analytics-b");
        assertThat(topology.group("core").clustersByPriority()).extracting(ClusterTopology.ClusterRef::id)
                .containsExactly("core-a", "core-b");
    }

    @Test
    void analyticsNarrowsTheGlobalHealthCheck() {
        assertThat(topology.group("analytics").healthCheck().getFailureThreshold()).isEqualTo(2);
        assertThat(topology.group("analytics").healthCheck().getIntervalMs()).isEqualTo(2000);
        assertThat(topology.group("core").healthCheck().getFailureThreshold()).isEqualTo(2);
    }

    @Test
    void eachBridgeDependsOnTheGroupItWritesToAndAcknowledgesManually() {
        assertBridge("orders-bridge", "core", "analytics");
        assertBridge("analytics-scorer", "analytics", "core");
    }

    @Test
    void auditExistsInBothGroupsUnderTheSameConsumerGroup() {
        ConsumerConfig core = properties.getConsumers().get("core-audit");
        ConsumerConfig analytics = properties.getConsumers().get("analytics-audit");

        assertThat(core.getTopic()).isEqualTo(analytics.getTopic());
        assertThat(core.getGroup()).isEqualTo(analytics.getGroup());
        assertThat(topology.groupOf(core).name()).isEqualTo("core");
        assertThat(topology.groupOf(analytics).name()).isEqualTo("analytics");
        assertThat(topology.group("core").topics()).contains("audit");
        assertThat(topology.group("analytics").topics()).contains("audit");
    }

    @Test
    void producersOfTheSharedTopicLiveInDifferentGroups() {
        assertThat(topology.groupOf(properties.getProducers().get("core-audit")).name()).isEqualTo("core");
        assertThat(topology.groupOf(properties.getProducers().get("analytics-audit")).name()).isEqualTo("analytics");
        assertThat(topology.groupOf(properties.getProducers().get("order-analytics")).name()).isEqualTo("analytics");
        assertThat(topology.groupOf(properties.getProducers().get("order-scores")).name()).isEqualTo("core");
    }

    private static void assertBridge(String name, String ownGroup, String dependency) {
        ConsumerConfig consumer = properties.getConsumers().get(name);
        assertThat(topology.groupOf(consumer).name()).isEqualTo(ownGroup);
        assertThat(consumer.getDependsOn()).containsExactly(dependency);
        // A record the group is up for yet does not take is held two minutes, not forever.
        assertThat(consumer.getDependsOnMaxHoldMs()).isEqualTo(120_000);
        AckPolicy policy = properties.resolveAckPolicy(consumer);
        assertThat(policy.ackMode()).isEqualTo(AckMode.MANUAL);
        assertThat(policy.starterAcknowledges()).isTrue();
    }
}
