package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAdminHelperTest {

    private MockedStatic<AdminClient> adminClientStatic;
    private AdminClient adminClient;

    @BeforeEach
    void setup() {
        adminClient = mock(AdminClient.class);
        adminClientStatic = mockStatic(AdminClient.class);
        adminClientStatic.when(() -> AdminClient.create(any(Map.class))).thenReturn(adminClient);
    }

    @AfterEach
    void tearDown() {
        adminClientStatic.close();
    }

    @Test
    void extractKafkaClientPropertiesStripsBinderConfigPrefix() {
        Map<String, String> env = Map.of(
                "spring.cloud.stream.kafka.binder.configuration.security.protocol", "SSL",
                "spring.cloud.stream.kafka.binder.configuration.ssl.truststore.location", "/certs/ts.p12",
                "spring.cloud.stream.kafka.binder.brokers", "kafka:9092",
                "some.unrelated.property", "value"
        );

        Map<String, String> kafkaProps = KafkaAdminHelper.extractKafkaClientProperties(env);

        assertThat(kafkaProps)
                .containsEntry("security.protocol", "SSL")
                .containsEntry("ssl.truststore.location", "/certs/ts.p12")
                .doesNotContainKey("brokers")
                .doesNotContainKey("some.unrelated.property");
    }

    @Test
    void extractKafkaClientPropertiesOnEmptyEnvReturnsEmpty() {
        assertThat(KafkaAdminHelper.extractKafkaClientProperties(Map.of())).isEmpty();
    }

    @Test
    void schemaRegistryUrlIsExtracted() {
        Map<String, String> env = Map.of(
                "spring.cloud.stream.kafka.binder.configuration.schema.registry.url", "http://sr:8081"
        );

        assertThat(KafkaAdminHelper.extractKafkaClientProperties(env))
                .containsEntry("schema.registry.url", "http://sr:8081");
    }

    @Test
    void probeClusterReturnsTrueWhenAdminClientResponds() {
        stubDescribeCluster("cid");

        assertThat(KafkaAdminHelper.probeCluster("kafka:9092", 1000, Map.of())).isTrue();
    }

    @Test
    void probeClusterReturnsFalseWhenAdminClientThrows() {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        KafkaFutureImpl<String> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new TimeoutException("broker down"));
        when(cluster.clusterId()).thenReturn(failed);

        assertThat(KafkaAdminHelper.probeCluster("kafka:9092", 1000, Map.of())).isFalse();
    }

    @Test
    void probeClusterDefaultTimeoutOverload() {
        stubDescribeCluster("cid");

        assertThat(KafkaAdminHelper.probeCluster("kafka:9092")).isTrue();
    }

    @Test
    void probeClusterTwoArgOverloadDefaultsKafkaProps() {
        stubDescribeCluster("cid");

        assertThat(KafkaAdminHelper.probeCluster("kafka:9092", 1000)).isTrue();
    }

    @Test
    void probeClusterByClusterNameUsesPropertiesEnvironment() {
        stubDescribeCluster("cid");

        KafkaClusterProperties props = singleClusterProperties();
        assertThat(KafkaAdminHelper.probeCluster("primary", props)).isTrue();
    }

    @Test
    void provisionTopicsCreatesMissingTopicsOnly() {
        stubListTopics(Set.of("orders"));
        stubCreateTopicsSucceeds();

        KafkaClusterProperties props = singleClusterProperties();
        addConsumer(props, "orders");
        addProducer(props, "events");

        KafkaAdminHelper.provisionTopics("primary", "kafka:9092", props);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());
        assertThat(captor.getValue())
                .extracting(NewTopic::name)
                .containsExactly("events");
    }

    @Test
    void provisionTopicsSkipsCreateWhenAllTopicsExist() {
        stubListTopics(Set.of("orders", "events"));

        KafkaClusterProperties props = singleClusterProperties();
        addConsumer(props, "orders");
        addProducer(props, "events");

        KafkaAdminHelper.provisionTopics("primary", "kafka:9092", props);

        verify(adminClient, never()).createTopics(any());
    }

    @Test
    void provisionTopicsHonorsConfiguredReplicationFactor() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("replication-factor", "3");
        KafkaClusterProperties props = singleClusterProperties();
        props.setDefaultEnvironment(Map.of("spring.cloud.stream.kafka.binder", nested));
        addProducer(props, "events");

        stubListTopics(Set.of());
        stubCreateTopicsSucceeds();

        KafkaAdminHelper.provisionTopics("primary", "kafka:9092", props);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());
        NewTopic topic = captor.getValue().iterator().next();
        assertThat(topic.replicationFactor()).isEqualTo((short) 3);
    }

    @Test
    void provisionTopicsHandlesEmptyTopicList() {
        KafkaClusterProperties props = singleClusterProperties();

        KafkaAdminHelper.provisionTopics("primary", "kafka:9092", props);

        verify(adminClient, never()).listTopics();
        verify(adminClient, never()).createTopics(any());
    }

    @Test
    void provisionTopicsSwallowsExceptions() {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.listTopics()).thenThrow(new RuntimeException("boom"));

        KafkaClusterProperties props = singleClusterProperties();
        addConsumer(props, "orders");

        KafkaAdminHelper.provisionTopics("primary", "kafka:9092", props);
    }

    @Test
    void createAdminClientWithoutExtraPropsUsesDefaults() {
        AdminClient result = KafkaAdminHelper.createAdminClient("kafka:9092", 1000);
        assertThat(result).isSameAs(adminClient);
    }

    @Test
    @SuppressWarnings("unchecked")
    void createAdminClientBoundsSocketConnectionSetupByTimeout() {
        KafkaAdminHelper.createAdminClient("kafka:9092", 1500, Map.of("security.protocol", "SSL"));

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        adminClientStatic.verify(() -> AdminClient.create(captor.capture()));
        Map<String, Object> config = captor.getValue();

        // The bug: this overload omitted socket.connection.setup.timeout.ms, so an
        // unreachable broker blocked on Kafka's 10s default instead of timeout-ms.
        assertThat(config)
                .containsEntry("request.timeout.ms", 1500)
                .containsEntry("default.api.timeout.ms", 1500)
                .containsEntry("socket.connection.setup.timeout.ms", 1500L)
                .containsEntry("socket.connection.setup.timeout.max.ms", 1500L)
                .containsEntry("security.protocol", "SSL");
    }

    private void stubDescribeCluster(String clusterId) {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(adminClient.describeCluster()).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture(clusterId));
    }

    private void stubListTopics(Set<String> existing) {
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(KafkaFuture.completedFuture(existing));
    }

    private void stubCreateTopicsSucceeds() {
        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(adminClient.createTopics(any())).thenReturn(createResult);
        when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    }

    private static KafkaClusterProperties singleClusterProperties() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        ClusterConfig cfg = new ClusterConfig();
        cfg.setBootstrapServers("kafka:9092");
        props.setClusters(Map.of("primary", cfg));
        return props;
    }

    private static void addConsumer(KafkaClusterProperties props, String topic) {
        ConsumerConfig c = new ConsumerConfig();
        c.setTopic(topic);
        Map<String, ConsumerConfig> existing = new LinkedHashMap<>(props.getConsumers());
        existing.put(topic + "-consumer", c);
        props.setConsumers(existing);
    }

    private static void addProducer(KafkaClusterProperties props, String topic) {
        ProducerConfig p = new ProducerConfig();
        p.setTopic(topic);
        Map<String, ProducerConfig> existing = new LinkedHashMap<>(props.getProducers());
        existing.put(topic + "-producer", p);
        props.setProducers(existing);
    }
}
