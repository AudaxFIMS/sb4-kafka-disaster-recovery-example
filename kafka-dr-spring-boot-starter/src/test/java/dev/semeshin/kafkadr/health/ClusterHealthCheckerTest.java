package dev.semeshin.kafkadr.health;

import dev.semeshin.kafkadr.config.AdminClientFactory;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterHealthCheckerTest {

    private KafkaClusterProperties props;
    private ActiveClusterManager mgr;
    private AdminClientFactory factory;
    private AdminClient adminClient;

    @BeforeEach
    void setup() {
        props = new KafkaClusterProperties();
        ClusterConfig primary = new ClusterConfig();
        primary.setBootstrapServers("kafka-primary:9092");
        primary.setPriority(1);
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("primary", primary);
        props.setClusters(clusters);

        mgr = mock(ActiveClusterManager.class);
        factory = mock(AdminClientFactory.class);
        adminClient = mock(AdminClient.class);
        when(factory.create(anyString(), anyInt(), any())).thenReturn(adminClient);
    }

    @Test
    void healthExposesActiveClusterAndPerClusterStatus() {
        when(mgr.getActiveCluster()).thenReturn("primary");
        when(mgr.getHealthStatuses()).thenReturn(Map.of(
                "primary", true,
                "secondary", false
        ));

        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);
        Health health = checker.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("activeCluster", "primary")
                .containsEntry("cluster.primary", "UP")
                .containsEntry("cluster.secondary", "DOWN");
    }

    @Test
    void basicProbeReportsHealthyWhenClusterIdResolves() {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("cid"));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", true);
    }

    @Test
    void basicProbeReportsUnhealthyOnException() {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        KafkaFutureImpl<String> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new TimeoutException("broker down"));
        when(cluster.clusterId()).thenReturn(failed);

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", false);
    }

    @Test
    void deepProbeReportsHealthyWhenAllConfiguredTopicsHaveLeaders() {
        props.getHealthCheck().setDeepProbe(true);
        props.getHealthCheck().setDeepProbeMinNodes(1);
        addConsumer("orders");

        stubDescribeCluster("cid");
        stubListTopics(Set.of("orders"));
        stubDescribeTopic("orders", partitionWithLeaderAndIsr(0, 1, List.of(1, 2)));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", true);
    }

    @Test
    void deepProbeReportsUnhealthyWhenLeaderMissing() {
        props.getHealthCheck().setDeepProbe(true);
        props.getHealthCheck().setDeepProbeMinNodes(1);
        addConsumer("orders");

        stubDescribeCluster("cid");
        stubListTopics(Set.of("orders"));
        TopicPartitionInfo orphan = new TopicPartitionInfo(0, null, List.of(node(1)), List.of(node(1)));
        stubDescribeTopic("orders", orphan);

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", false);
    }

    @Test
    void deepProbeReportsUnhealthyWhenIsrBelowThreshold() {
        props.getHealthCheck().setDeepProbe(true);
        props.getHealthCheck().setDeepProbeMinNodes(1);
        props.getHealthCheck().setDeepProbeMinIsr(2);
        addConsumer("orders");

        stubDescribeCluster("cid");
        stubListTopics(Set.of("orders"));
        stubDescribeTopic("orders", partitionWithLeaderAndIsr(0, 1, List.of(1)));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", false);
    }

    @Test
    void deepProbeIgnoresTopicsThatDontExistOnCluster() {
        props.getHealthCheck().setDeepProbe(true);
        addConsumer("only-on-other-cluster");

        stubDescribeCluster("cid");
        stubListTopics(Set.of("__consumer_offsets"));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", true);
    }

    @Test
    void deepProbeReportsUnhealthyWhenAdminClientThrows() {
        props.getHealthCheck().setDeepProbe(true);
        addConsumer("orders");

        when(adminClient.describeCluster()).thenThrow(new RuntimeException("boom"));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", false);
    }

    @Test
    void deepProbeWithNoConfiguredTopicsReturnsHealthy() {
        props.getHealthCheck().setDeepProbe(true);

        stubDescribeCluster("cid");

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", true);
    }

    @Test
    void factoryReceivesKafkaClientPropertiesExtractedFromEnvironment() {
        props.setDefaultEnvironment(Map.of(
                "spring.cloud.stream.kafka.binder.configuration.security.protocol", "SSL"
        ));
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("cid"));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(factory).create(eq("kafka-primary:9092"), anyInt(), captor.capture());
        assertThat(captor.getValue()).containsEntry("security.protocol", "SSL");
    }

    private void addConsumer(String topic) {
        KafkaClusterProperties.ConsumerConfig c = new KafkaClusterProperties.ConsumerConfig();
        c.setTopic(topic);
        c.setHandler("h");
        props.setConsumers(Map.of(topic + "-consumer", c));
    }

    private void stubDescribeCluster(String cid) {
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(result);
        when(result.clusterId()).thenReturn(KafkaFuture.completedFuture(cid));
    }

    private void stubListTopics(Set<String> topics) {
        ListTopicsResult result = mock(ListTopicsResult.class);
        when(adminClient.listTopics()).thenReturn(result);
        when(result.names()).thenReturn(KafkaFuture.completedFuture(topics));
    }

    private void stubDescribeTopic(String topic, TopicPartitionInfo partition) {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(adminClient.describeTopics(any(Collection.class))).thenReturn(result);
        TopicDescription desc = new TopicDescription(topic, false, List.of(partition));
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, desc)));
    }

    private static TopicPartitionInfo partitionWithLeaderAndIsr(int partition, int leaderId, List<Integer> isrIds) {
        Node leader = node(leaderId);
        List<Node> isr = isrIds.stream().map(ClusterHealthCheckerTest::node).toList();
        return new TopicPartitionInfo(partition, leader, isr, isr);
    }

    private static Node node(int id) {
        return new Node(id, "host-" + id, 9092);
    }
}
