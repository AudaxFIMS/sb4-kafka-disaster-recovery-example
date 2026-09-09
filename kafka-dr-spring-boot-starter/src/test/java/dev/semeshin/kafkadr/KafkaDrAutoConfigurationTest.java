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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
    void customizerInstallsTheRebalanceListenerOnlyWhenSeekByTimestampIsOn() {
        KafkaClusterProperties properties = propertiesWith(consumerConfig("plain", "orders", "g1", false));
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);

        ContainerProperties without = configure(
                new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(properties, tracker), "orders", "g1");
        assertThat(without.getConsumerRebalanceListener()).isNull();

        properties.getFailover().setSeekByTimestamp(true);
        ContainerProperties with = configure(
                new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(properties, tracker), "orders", "g1");
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
                new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(
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
                new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(
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
                new KafkaDrAutoConfiguration().kafkaDrContainerCustomizer(
                        propertiesWith(consumerConfig("orders", "orders", "g1", false)),
                        new LastProcessedTimestampTracker(null)),
                "orders", "g1");

        assertThat(props.isAsyncAcks()).isEqualTo(defaults.isAsyncAcks());
        assertThat(props.isSyncCommits()).isEqualTo(defaults.isSyncCommits());
        assertThat(props.getAckCount()).isEqualTo(defaults.getAckCount());
        assertThat(props.getAckTime()).isEqualTo(defaults.getAckTime());
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
