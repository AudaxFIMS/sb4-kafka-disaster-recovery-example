package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.DefaultAdminClientFactory;
import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import dev.semeshin.kafkadr.routing.FailoverStateStore;
import dev.semeshin.kafkadr.routing.InMemoryFailoverStateStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaListenerContainerCustomizer;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDrAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaDrAutoConfiguration.class));

    /**
     * Runner with kafka-dr enabled: component scan pulls in beans depending on
     * Spring Cloud Stream infrastructure, so those collaborators are mocked.
     */
    private final ApplicationContextRunner enabledRunner = runner
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class))
            .withPropertyValues(
                    "kafka-dr.enabled=true",
                    "kafka-dr.clusters.primary.bootstrap-servers=kafka-primary:9092")
            .withBean(StreamBridge.class, () -> mock(StreamBridge.class))
            .withBean(BindingsLifecycleController.class, () -> mock(BindingsLifecycleController.class))
            .withBean(BinderFactory.class, () -> mock(BinderFactory.class))
            .withBean(BindingServiceProperties.class, () -> mock(BindingServiceProperties.class));

    @Test
    void autoConfigurationDoesNotLoadWhenEnabledPropertyAbsent() {
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(IdempotencyStore.class));
    }

    @Test
    void autoConfigurationDoesNotLoadWhenEnabledIsFalse() {
        runner.withPropertyValues("kafka-dr.enabled=false")
                .run(ctx -> assertThat(ctx).doesNotHaveBean(IdempotencyStore.class));
    }

    @Test
    void idempotencyStoreIsRegisteredByDefault() {
        try (MockedStatic<KafkaAdminHelper> ignored = mockStatic(KafkaAdminHelper.class)) {
            enabledRunner.run(ctx -> assertThat(ctx).hasSingleBean(InMemoryIdempotencyStore.class));
        }
    }

    @Test
    void idempotencyStoreIsNotRegisteredWhenDisabled() {
        try (MockedStatic<KafkaAdminHelper> ignored = mockStatic(KafkaAdminHelper.class)) {
            enabledRunner.withPropertyValues("kafka-dr.idempotency.enabled=false")
                    .run(ctx -> assertThat(ctx).doesNotHaveBean(IdempotencyStore.class));
        }
    }

    @Test
    void inMemoryIdempotencyStoreFactoryBeanCreatesValidInstance() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        InMemoryIdempotencyStore store = cfg.inMemoryIdempotencyStore(new KafkaClusterProperties());

        assertThat(store).isNotNull();
        Message<?> msg = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "id-1")
                .build();
        assertThat(store.tryProcess("primary", "c", msg)).isTrue();
        assertThat(store.tryProcess("primary", "c", msg)).isFalse();
    }

    @Test
    void inMemoryIdempotencyStoreUsesTheConfiguredTtl() {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        properties.getIdempotency().setTtlSeconds(90);

        InMemoryIdempotencyStore store = new KafkaDrAutoConfiguration().inMemoryIdempotencyStore(properties);

        assertThat(org.springframework.test.util.ReflectionTestUtils.getField(store, "ttlSeconds")).isEqualTo(90L);
    }

    @Test
    void inMemoryFailoverStateStoreFactoryBeanCreatesValidInstance() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        InMemoryFailoverStateStore store = cfg.inMemoryFailoverStateStore();

        assertThat(store).isNotNull();
        store.save(new FailoverStateStore.FailoverState("primary", Instant.parse("2025-01-15T14:00:00Z")));
        Optional<FailoverStateStore.FailoverState> loaded = store.load();
        assertThat(loaded).isPresent();
        assertThat(loaded.get().activeCluster()).isEqualTo("primary");
    }

    @Test
    void defaultAdminClientFactoryBeanIsRegistered() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        DefaultAdminClientFactory factory = cfg.defaultAdminClientFactory();

        assertThat(factory).isNotNull();
    }

    @Test
    void exactlyOneListenerContainerCustomizerIsExposed() {
        // KafkaBinderConfiguration injects a single ListenerContainerCustomizer. A second
        // bean of this type makes the binder child context fail to start with
        // "required a single bean, but 2 were found", which only shows up when a binder is
        // actually created — not in a unit test that calls the factory method directly.
        enabledRunner.run(ctx ->
                assertThat(ctx.getBeansOfType(ListenerContainerCustomizer.class)).hasSize(1));
    }

    @Test
    void producerAsksTheHealthCheckerWhetherAClusterAnswers() {
        enabledRunner.run(ctx -> {
            assertThat(ctx).hasSingleBean(dev.semeshin.kafkadr.producer.ClusterReachability.class);
            // Without it every "topic not present in metadata" would fail the cluster over.
            assertThat(org.springframework.test.util.ReflectionTestUtils.getField(
                    ctx.getBean(dev.semeshin.kafkadr.producer.ResilientProducer.class), "reachability"))
                    .isSameAs(ctx.getBean(dev.semeshin.kafkadr.health.ClusterHealthChecker.class));
        });
    }

    @Test
    void theTopologyIsBuiltOnceAndShared() {
        enabledRunner.run(ctx -> {
            assertThat(ctx).hasSingleBean(dev.semeshin.kafkadr.config.ClusterTopology.class);
            assertThat(ctx.getBean(dev.semeshin.kafkadr.config.ClusterTopology.class).clusters()).isNotEmpty();
        });
    }

    @Test
    void containersThatAreNotDrConsumersGetNoSeekListener() {
        KafkaClusterProperties properties = propertiesWith(consumerConfig("orders", "orders", "g1", false));
        properties.getFailover().setSeekByTimestamp(true);
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        // A watermark nothing advances any more: seeking with it could only move a partition back.
        tracker.update("payments", 0, 1000L);

        ContainerProperties unknown = configure(customizerFor(properties, tracker), "payments", "other-group");

        assertThat(unknown.getConsumerRebalanceListener()).isNull();
    }

    @Test
    void ambiguousContainerGetsNeitherSettingsNorASeekListener() {
        KafkaClusterProperties.ConsumerConfig core = consumerConfig("orders-core", "orders", "shared", true);
        core.setClusterGroup("core");
        KafkaClusterProperties.ConsumerConfig analytics = consumerConfig("orders-analytics", "orders", "shared", true);
        analytics.setClusterGroup("analytics");
        KafkaClusterProperties properties = propertiesWith(core, analytics);
        java.util.Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new java.util.LinkedHashMap<>();
        groups.put("core", clusterGroup("primary", "core-a:9092"));
        groups.put("analytics", clusterGroup("dc1", "an-a:9092"));
        properties.setClusterGroups(groups);
        properties.getFailover().setSeekByTimestamp(true);

        ContainerProperties ambiguous = configure(
                customizerFor(properties, new LastProcessedTimestampTracker(null)), "orders", "shared");

        assertThat(ambiguous.getConsumerRebalanceListener()).isNull();
        assertThat(ambiguous.isSubBatchPerPartition()).isFalse();
    }

    @Test
    void customizerInstallsTheRebalanceListenerOnlyWhenSeekByTimestampIsOn() {
        KafkaClusterProperties properties = propertiesWith(consumerConfig("plain", "orders", "g1", false));
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);

        ContainerProperties without = configure(
                customizerFor(properties, tracker), "orders", "g1");
        assertThat(without.getConsumerRebalanceListener()).isNull();

        properties.getFailover().setSeekByTimestamp(true);
        ContainerProperties with = configure(
                customizerFor(properties, tracker), "orders", "g1");
        assertThat(with.getConsumerRebalanceListener()).isInstanceOf(ConsumerAwareRebalanceListener.class);

        @SuppressWarnings("unchecked")
        Consumer<Object, Object> consumer = mock(Consumer.class);
        ((ConsumerAwareRebalanceListener) with.getConsumerRebalanceListener())
                .onPartitionsAssigned(consumer, List.<TopicPartition>of());
    }

    @Test
    void customizerEnablesSubBatchPerPartitionOnlyForBatchingConsumers() {
        KafkaClusterProperties properties = propertiesWith(
                consumerConfig("batched", "orders", "g1", true),
                consumerConfig("plain", "payments", "g2", false));

        ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer =
                customizerFor(
                        properties, new LastProcessedTimestampTracker(null));

        // A poll is grouped by partition, so without sub-batches a failure in the first
        // partition truncates the commit prefix for every partition behind it.
        assertThat(configure(customizer, "orders", "g1").isSubBatchPerPartition()).isTrue();
        assertThat(configure(customizer, "payments", "g2").isSubBatchPerPartition()).isFalse();
        // A container the starter did not configure must be left untouched.
        assertThat(configure(customizer, "unknown", "g3").isSubBatchPerPartition()).isFalse();
    }

    @Test
    void customizerAppliesTheAckSettingsTheBinderCannotExpress() {
        KafkaClusterProperties.ConsumerConfig consumer = consumerConfig("orders", "orders", "g1", false);
        consumer.getAck().setAsyncAcks(true);
        consumer.getAck().setSyncCommits(false);
        consumer.getAck().setCount(50);
        consumer.getAck().setTime(2000L);

        // None of these exist in KafkaConsumerProperties, so YAML cannot reach them through
        // the binder — the customizer is the only way in.
        ContainerProperties props = configure(
                customizerFor(
                        propertiesWith(consumer), new LastProcessedTimestampTracker(null)),
                "orders", "g1");

        assertThat(props.isAsyncAcks()).isTrue();
        assertThat(props.isSyncCommits()).isFalse();
        assertThat(props.getAckCount()).isEqualTo(50);
        assertThat(props.getAckTime()).isEqualTo(2000L);
    }

    @Test
    void ackSettingsLeftUnsetKeepTheSpringKafkaDefaults() {
        ContainerProperties defaults = new ContainerProperties("orders");

        ContainerProperties props = configure(
                customizerFor(
                        propertiesWith(consumerConfig("orders", "orders", "g1", false)),
                        new LastProcessedTimestampTracker(null)),
                "orders", "g1");

        assertThat(props.isAsyncAcks()).isEqualTo(defaults.isAsyncAcks());
        assertThat(props.isSyncCommits()).isEqualTo(defaults.isSyncCommits());
        assertThat(props.getAckCount()).isEqualTo(defaults.getAckCount());
        assertThat(props.getAckTime()).isEqualTo(defaults.getAckTime());
    }

    @Test
    void customizerMatchesContainersByBindingNameAcrossClusterGroups() {
        KafkaClusterProperties.ConsumerConfig core = consumerConfig("orders-core", "orders", "shared", true);
        core.setClusterGroup("core");
        KafkaClusterProperties.ConsumerConfig analytics = consumerConfig("orders-analytics", "orders", "shared", false);
        analytics.setClusterGroup("analytics");
        analytics.getAck().setCount(7);
        KafkaClusterProperties properties = propertiesWith(core, analytics);
        java.util.Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new java.util.LinkedHashMap<>();
        groups.put("core", clusterGroup("primary", "core-a:9092"));
        groups.put("analytics", clusterGroup("dc1", "an-a:9092"));
        properties.setClusterGroups(groups);
        groups.get("analytics").getFailover().setSeekByTimestamp(true);

        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.forConsumer("orders-core").update("orders", 0, 9000L);
        tracker.forConsumer("orders-analytics").update("orders", 0, 1000L);
        KafkaListenerContainerCustomizer customizer = customizerFor(
                properties, tracker);

        // Same topic and consumer group in two Kafkas: only the binding name tells them apart.
        ContainerProperties coreProps = configure(customizer, "orders", "shared", "ordersCoreCorePrimary-in-0");
        assertThat(coreProps.isSubBatchPerPartition()).isTrue();
        assertThat(coreProps.getConsumerRebalanceListener()).isNull();

        ContainerProperties analyticsProps = configure(customizer, "orders", "shared", "ordersAnalyticsAnalyticsDc1-in-0");
        assertThat(analyticsProps.isSubBatchPerPartition()).isFalse();
        assertThat(analyticsProps.getAckCount()).isEqualTo(7);
        assertThat(analyticsProps.getConsumerRebalanceListener()).isInstanceOf(ConsumerAwareRebalanceListener.class);
        // The listener seeks with the watermark of its own consumer, not with that of another
        // consumer of the same topic.
        @SuppressWarnings("unchecked")
        Consumer<Object, Object> kafkaConsumer = mock(Consumer.class);
        TopicPartition tp = new TopicPartition("orders", 0);
        when(kafkaConsumer.offsetsForTimes(any())).thenReturn(java.util.Map.of());
        ((ConsumerAwareRebalanceListener) analyticsProps.getConsumerRebalanceListener())
                .onPartitionsAssigned(kafkaConsumer, List.of(tp));
        verify(kafkaConsumer).offsetsForTimes(java.util.Map.of(tp, 1000L));

        // Without a binding name the pair is ambiguous, and nothing is guessed.
        ContainerProperties ambiguous = configure(customizer, "orders", "shared");
        assertThat(ambiguous.isSubBatchPerPartition()).isFalse();
        assertThat(ambiguous.getAckCount()).isEqualTo(new ContainerProperties("orders").getAckCount());
    }

    private static ContainerProperties configure(KafkaListenerContainerCustomizer customizer,
                                                 String destination, String group, String bindingName) {
        AbstractMessageListenerContainer<?, ?> container = mock(AbstractMessageListenerContainer.class);
        ContainerProperties props = new ContainerProperties(destination);
        when(container.getContainerProperties()).thenReturn(props);
        ExtendedConsumerProperties<KafkaConsumerProperties> extended =
                new ExtendedConsumerProperties<>(new KafkaConsumerProperties());
        extended.populateBindingName(bindingName);
        customizer.configure(container, destination, group, extended);
        return props;
    }

    private static KafkaClusterProperties.ClusterGroupConfig clusterGroup(String cluster, String brokers) {
        KafkaClusterProperties.ClusterConfig cfg = new KafkaClusterProperties.ClusterConfig();
        cfg.setBootstrapServers(brokers);
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        group.setClusters(java.util.Map.of(cluster, cfg));
        return group;
    }

    /** The customizer as the auto-configuration builds it, with the topology resolved once. */
    private static KafkaListenerContainerCustomizer customizerFor(KafkaClusterProperties properties,
                                                                  LastProcessedTimestampTracker tracker) {
        return new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(properties, properties.topology(), tracker);
    }

    private static ContainerProperties configure(
            ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer,
            String destination, String group) {
        AbstractMessageListenerContainer<?, ?> container = mock(AbstractMessageListenerContainer.class);
        ContainerProperties props = new ContainerProperties(destination);
        when(container.getContainerProperties()).thenReturn(props);
        customizer.configure(container, destination, group);
        return props;
    }

    private static KafkaClusterProperties propertiesWith(KafkaClusterProperties.ConsumerConfig... consumers) {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        java.util.Map<String, KafkaClusterProperties.ConsumerConfig> map = new java.util.LinkedHashMap<>();
        for (KafkaClusterProperties.ConsumerConfig consumer : consumers) {
            map.put(consumer.getName(), consumer);
        }
        properties.setConsumers(map);
        return properties;
    }

    private static KafkaClusterProperties.ConsumerConfig consumerConfig(
            String name, String topic, String group, boolean batch) {
        KafkaClusterProperties.ConsumerConfig consumer = new KafkaClusterProperties.ConsumerConfig();
        consumer.setName(name);
        consumer.setTopic(topic);
        consumer.setGroup(group);
        consumer.getBatch().setEnabled(batch);
        return consumer;
    }
}
