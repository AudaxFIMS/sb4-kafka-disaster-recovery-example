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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
    void reachabilityProbesThatClusterOnlyAndReportsNothing() {
        ClusterConfig secondary = new ClusterConfig();
        secondary.setBootstrapServers("kafka-secondary:9092");
        secondary.setPriority(2);
        props.getClusters().put("secondary", secondary);
        props.getHealthCheck().setTimeoutMs(1234);
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("cid"));
        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);

        assertThat(checker.isReachable("secondary")).isTrue();

        verify(factory).create(eq("kafka-secondary:9092"), eq(1234), any());
        verify(factory, never()).create(eq("kafka-primary:9092"), anyInt(), any());
        // A producer's question, not a health round: the manager hears nothing of it.
        verify(mgr, never()).reportHealth(anyString(), anyBoolean());
    }

    @Test
    void reachabilityIsFalseForAClusterThatDoesNotAnswerOrIsUnknown() {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        KafkaFuture<String> failed = KafkaFuture.completedFuture(null).thenApply(v -> {
            throw new org.apache.kafka.common.errors.TimeoutException("no answer");
        });
        when(cluster.clusterId()).thenReturn(failed);
        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);

        assertThat(checker.isReachable("primary")).isFalse();
        assertThat(checker.isReachable("no-such-cluster")).isFalse();
        verify(factory, times(1)).create(anyString(), anyInt(), any());
    }

    @Test
    void topicLookupTellsAMissingTopicFromAClusterThatDoesNotAnswer() {
        org.apache.kafka.clients.admin.DescribeTopicsResult topics = mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class);
        when(adminClient.describeTopics(any(java.util.Collection.class))).thenReturn(topics);
        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);

        when(topics.allTopicNames()).thenReturn(failedFuture(
                new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("no such topic")));
        assertThat(checker.lacksTopic("primary", "orders")).isTrue();

        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        assertThat(checker.lacksTopic("primary", "orders")).isFalse();

        // Not answering is not "the topic is missing".
        when(topics.allTopicNames()).thenReturn(failedFuture(
                new org.apache.kafka.common.errors.TimeoutException("no answer")));
        assertThat(checker.lacksTopic("primary", "orders")).isFalse();
        assertThat(checker.lacksTopic("no-such-cluster", "orders")).isFalse();
    }

    private static <T> KafkaFuture<T> failedFuture(RuntimeException cause) {
        org.apache.kafka.common.internals.KafkaFutureImpl<T> future = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        future.completeExceptionally(cause);
        return future;
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
    void allClustersAreProbedAndReported() {
        ClusterConfig secondary = new ClusterConfig();
        secondary.setBootstrapServers("kafka-secondary:9092");
        secondary.setPriority(2);
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("primary", props.getClusters().get("primary"));
        clusters.put("secondary", secondary);
        props.setClusters(clusters);

        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("cid"));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(mgr).reportHealth("primary", true);
        verify(mgr).reportHealth("secondary", true);
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

    @Test
    void eachGroupIsProbedWithItsOwnSettingsAndTopics() {
        props.setClusters(new LinkedHashMap<>());
        props.getHealthCheck().setTimeoutMs(2000);
        props.getHealthCheck().setDeepProbe(true);
        props.getHealthCheck().setDeepProbeMinNodes(1);

        KafkaClusterProperties.ClusterGroupConfig core = new KafkaClusterProperties.ClusterGroupConfig();
        core.setClusters(Map.of("primary", clusterCfg("core-a:9092")));
        KafkaClusterProperties.ClusterGroupConfig analytics = new KafkaClusterProperties.ClusterGroupConfig();
        analytics.setClusters(Map.of("dc1", clusterCfg("an-a:9092")));
        analytics.getHealthCheck().setTimeoutMs(750L);
        analytics.getHealthCheck().setDeepProbe(false);
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", core);
        groups.put("analytics", analytics);
        props.setClusterGroups(groups);

        KafkaClusterProperties.ConsumerConfig orders = new KafkaClusterProperties.ConsumerConfig();
        orders.setTopic("orders");
        orders.setClusterGroup("core");
        KafkaClusterProperties.ConsumerConfig scores = new KafkaClusterProperties.ConsumerConfig();
        scores.setTopic("scores");
        scores.setClusterGroup("analytics");
        props.setConsumers(Map.of("orders", orders, "scores", scores));

        stubDescribeCluster("cid");
        stubListTopics(Set.of("orders", "scores"));
        stubDescribeTopic("orders", partitionWithLeaderAndIsr(0, 1, List.of(1)));

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        verify(factory).create(eq("core-a:9092"), eq(2000), any());
        verify(factory).create(eq("an-a:9092"), eq(750), any());
        // Deep probe only for core, and only over core's topics: scores lives in another Kafka.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<String>> topics = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).describeTopics(topics.capture());
        assertThat(topics.getValue()).containsExactly("orders");
        verify(mgr).reportHealth("core-primary", true);
        verify(mgr).reportHealth("analytics-dc1", true);
    }

    @Test
    void healthIsReportedPerGroupWhenSeveralAreConfigured() {
        props.setClusters(new LinkedHashMap<>());
        props.setClusterGroups(twoGroups(5000L, 5000L));
        when(mgr.getActiveCluster("core")).thenReturn("core-secondary");
        when(mgr.getActiveCluster("analytics")).thenReturn("analytics-dc1");
        when(mgr.getHealthStatuses("core")).thenReturn(Map.of("core-primary", false));
        when(mgr.getHealthStatuses("analytics")).thenReturn(Map.of("analytics-dc1", true));

        Health health = new ClusterHealthChecker(props, mgr, factory).health();

        assertThat(health.getDetails())
                .containsEntry("group.core.activeCluster", "core-secondary")
                .containsEntry("group.core.cluster.primary", "DOWN")
                .containsEntry("group.analytics.activeCluster", "analytics-dc1")
                .containsEntry("group.analytics.cluster.dc1", "UP")
                .doesNotContainKey("activeCluster");
    }

    @Test
    void eachGroupIsScheduledAtItsOwnIntervalUntilStopped() throws Exception {
        props.setClusters(new LinkedHashMap<>());
        props.setClusterGroups(twoGroups(50L, 60_000L));
        stubDescribeCluster("cid");

        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);
        checker.start();
        try {
            assertThat(checker.isRunning()).isTrue();
            Thread.sleep(600);
        } finally {
            checker.stop();
        }
        assertThat(checker.isRunning()).isFalse();

        // The first round of every group runs at once; only core comes back every 50 ms.
        verify(mgr, atLeast(4)).reportHealth("core-primary", true);
        verify(mgr, times(1)).reportHealth("analytics-dc1", true);

        clearInvocations(mgr);
        Thread.sleep(200);
        verify(mgr, never()).reportHealth(anyString(), anyBoolean());
        checker.shutdown();
    }

    @Test
    void roundInterruptedByStopReportsNothing() throws Exception {
        props.getHealthCheck().setTimeoutMs(5000);
        DescribeClusterResult slow = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(slow);
        when(slow.clusterId()).thenReturn(new KafkaFutureImpl<>());   // never completes
        java.util.concurrent.CountDownLatch probing = new java.util.concurrent.CountDownLatch(1);
        when(factory.create(anyString(), anyInt(), any())).thenAnswer(inv -> {
            probing.countDown();
            return adminClient;
        });

        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);
        checker.start();
        assertThat(probing.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        checker.stop();
        Thread.sleep(300);
        checker.shutdown();

        // An interrupted wait is no verdict on the cluster; reporting it as a failure would fail
        // the group over — and persist that failover — on the way down.
        verify(mgr, never()).reportHealth(anyString(), anyBoolean());
    }

    @Test
    void eachGroupIsProbedFromItsOwnPool() {
        props.setClusters(new LinkedHashMap<>());
        props.setClusterGroups(twoGroups(5000L, 5000L));
        stubDescribeCluster("cid");
        Map<String, String> threadByBrokers = new java.util.concurrent.ConcurrentHashMap<>();
        when(factory.create(anyString(), anyInt(), any())).thenAnswer(inv -> {
            threadByBrokers.put(inv.getArgument(0), Thread.currentThread().getName());
            return adminClient;
        });

        ClusterHealthChecker checker = new ClusterHealthChecker(props, mgr, factory);
        checker.checkAllClusters();
        checker.shutdown();

        // Hanging probes of one group can then only ever queue behind each other.
        assertThat(threadByBrokers.get("core-a:9092")).startsWith("kafka-dr-health-probe-core-");
        assertThat(threadByBrokers.get("an-a:9092")).startsWith("kafka-dr-health-probe-analytics-");
    }

    @Test
    void clustersAreReportedInPriorityOrderNotDeclarationOrder() {
        ClusterConfig standby = new ClusterConfig();
        standby.setBootstrapServers("kafka-standby:9092");
        standby.setPriority(2);
        ClusterConfig main = new ClusterConfig();
        main.setBootstrapServers("kafka-main:9092");
        main.setPriority(1);
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("standby", standby);   // declared first
        clusters.put("main", main);
        props.setClusters(clusters);
        stubDescribeCluster("cid");

        new ClusterHealthChecker(props, mgr, factory).checkAllClusters();

        // The initial election takes the first healthy report: it has to be the best cluster.
        org.mockito.InOrder order = org.mockito.Mockito.inOrder(mgr);
        order.verify(mgr).reportHealth("main", true);
        order.verify(mgr).reportHealth("standby", true);
    }

    private static Map<String, KafkaClusterProperties.ClusterGroupConfig> twoGroups(long coreInterval,
                                                                                   long analyticsInterval) {
        KafkaClusterProperties.ClusterGroupConfig core = new KafkaClusterProperties.ClusterGroupConfig();
        core.setClusters(Map.of("primary", clusterCfg("core-a:9092")));
        core.getHealthCheck().setIntervalMs(coreInterval);
        KafkaClusterProperties.ClusterGroupConfig analytics = new KafkaClusterProperties.ClusterGroupConfig();
        analytics.setClusters(Map.of("dc1", clusterCfg("an-a:9092")));
        analytics.getHealthCheck().setIntervalMs(analyticsInterval);
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", core);
        groups.put("analytics", analytics);
        return groups;
    }

    private static ClusterConfig clusterCfg(String brokers) {
        ClusterConfig cfg = new ClusterConfig();
        cfg.setBootstrapServers(brokers);
        return cfg;
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
