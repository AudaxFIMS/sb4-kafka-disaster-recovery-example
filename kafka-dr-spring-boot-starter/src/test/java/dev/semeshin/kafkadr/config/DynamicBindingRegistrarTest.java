package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.consumer.DependencyGate;
import dev.semeshin.kafkadr.consumer.IdempotentConsumer;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import dev.semeshin.kafkadr.routing.DependencyGuard;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DynamicBindingRegistrarTest {

    private DefaultListableBeanFactory registry;
    private StandardEnvironment environment;
    private MockedStatic<KafkaAdminHelper> staticHelper;

    @BeforeEach
    void setup() {
        registry = new DefaultListableBeanFactory();
        environment = new StandardEnvironment();
        staticHelper = mockStatic(KafkaAdminHelper.class);
    }

    @AfterEach
    void tearDown() {
        staticHelper.close();
    }

    @Test
    void doesNothingWhenDisabled() {
        loadProperties(Map.of("kafka-dr.enabled", "false"));

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.primary.type")).isNull();
    }

    @Test
    void doesNothingWhenNoClustersConfigured() {
        loadProperties(Map.of("kafka-dr.enabled", "true"));

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.function.definition")).isNull();
    }

    @Test
    void generatesBinderPropertiesForEachCluster() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.primary.type")).isEqualTo("kafka");
        assertThat(environment.getProperty("spring.cloud.stream.binders.secondary.type")).isEqualTo("kafka");
        assertThat(environment.getProperty(
                "spring.cloud.stream.binders.primary.environment.spring.cloud.stream.kafka.binder.brokers"))
                .isEqualTo("kafka-primary:9092");
        assertThat(environment.getProperty(
                "spring.cloud.stream.binders.secondary.environment.spring.cloud.stream.kafka.binder.brokers"))
                .isEqualTo("kafka-secondary:9092");
    }

    @Test
    void generatesConsumerBindingsForEveryClusterTopicPair() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersPrimary-in-0.destination"))
                .isEqualTo("orders");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersPrimary-in-0.group"))
                .isEqualTo("dr-group");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersPrimary-in-0.binder"))
                .isEqualTo("primary");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersPrimary-in-0.consumer.auto-startup"))
                .isEqualTo("false");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersSecondary-in-0.binder"))
                .isEqualTo("secondary");
    }

    @Test
    void debugFlagIsPushedIntoTheAdminHelper() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.debug.enable", "true");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        staticHelper.verify(() -> KafkaAdminHelper.setDebugEnabled(true));
    }

    @Test
    void functionDefinitionIncludesOnlyReachableClusters() {
        loadProperties(twoClustersWithConsumer());
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        registrar().postProcessBeanDefinitionRegistry(registry);

        String defs = environment.getProperty("spring.cloud.function.definition");
        assertThat(defs).isEqualTo("ordersPrimary");
    }

    @Test
    void registersFunctionBeansForAllClustersIncludingUnreachable() {
        loadProperties(twoClustersWithConsumer());
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.containsBeanDefinition("ordersPrimary")).isTrue();
        assertThat(registry.containsBeanDefinition("ordersSecondary")).isTrue();
    }

    @Test
    void producerBindingsAreGeneratedForBothBaseAndOutZero() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        props.put("kafka-dr.producers.events.content-type", "json");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.bindings.events.destination"))
                .isEqualTo("events");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.events-out-0.destination"))
                .isEqualTo("events");
    }

    @Test
    void perClusterEnvironmentOverridesDefaultEnvironment() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.default-environment.spring.cloud.stream.kafka.binder.configuration.schema.registry.url",
                "http://default-sr:8081");
        props.put("kafka-dr.clusters.primary.environment.spring.cloud.stream.kafka.binder.configuration.schema.registry.url",
                "http://primary-sr:8081");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.binders.primary.environment.spring.cloud.stream.kafka.binder.configuration.schema.registry.url"))
                .isEqualTo("http://primary-sr:8081");
        assertThat(environment.getProperty(
                "spring.cloud.stream.binders.secondary.environment.spring.cloud.stream.kafka.binder.configuration.schema.registry.url"))
                .isEqualTo("http://default-sr:8081");
    }

    @Test
    void removesSpringBootKafkaAdminBeanWhenPresent() {
        GenericBeanDefinition kafkaAdmin = new GenericBeanDefinition();
        kafkaAdmin.setBeanClassName("org.springframework.kafka.core.KafkaAdmin");
        registry.registerBeanDefinition("kafkaAdmin", kafkaAdmin);
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.containsBeanDefinition("kafkaAdmin")).isFalse();
    }

    @Test
    void consumerConfigurationPropertiesArePassedToBinding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.properties.configuration.value.deserializer",
                "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.kafka.bindings.ordersPrimary-in-0.consumer.configuration.value.deserializer"))
                .isEqualTo("io.confluent.kafka.serializers.KafkaAvroDeserializer");
    }

    @Test
    void producerConfigurationPropertiesArePassedToBinding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        props.put("kafka-dr.producers.events.properties.configuration.acks", "1");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.kafka.bindings.events.producer.configuration.acks"))
                .isEqualTo("1");
    }

    @Test
    void provisionsTopicsWhenAutoCreateTopicsEnabled() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.auto-create-topics", "true");
        loadProperties(props);
        allClustersReachable();
        staticHelper.when(() -> KafkaAdminHelper.provisionTopics(anyString(), anyString(),
                        any(KafkaClusterProperties.class), any(ClusterTopology.class)))
                .thenAnswer(inv -> null);

        registrar().postProcessBeanDefinitionRegistry(registry);

        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("primary"), eq("kafka-primary:9092"),
                any(KafkaClusterProperties.class), any(ClusterTopology.class)));
        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("secondary"), eq("kafka-secondary:9092"),
                any(KafkaClusterProperties.class), any(ClusterTopology.class)));
    }

    @Test
    void nativeConsumerContentTypeEnablesNativeDecoding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.content-type", "native");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.ordersPrimary-in-0.consumer.use-native-decoding"))
                .isEqualTo("true");
    }

    @Test
    void nativeProducerContentTypeEnablesNativeEncoding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        props.put("kafka-dr.producers.events.content-type", "native");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.events.producer.use-native-encoding"))
                .isEqualTo("true");
        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.events-out-0.producer.use-native-encoding"))
                .isEqualTo("true");
    }

    @Test
    void producerBindingsAreAlwaysSynchronous() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        // Every failure the producer acts on surfaces only from a synchronous send.
        assertThat(environment.getProperty("spring.cloud.stream.kafka.bindings.events.producer.sync"))
                .isEqualTo("true");
        assertThat(environment.getProperty("spring.cloud.stream.kafka.bindings.events-out-0.producer.sync"))
                .isEqualTo("true");
    }

    @Test
    void asynchronousProducerIsRejected() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        props.put("kafka-dr.default-producer-properties.sync", "false");
        loadProperties(props);
        allClustersReachable();

        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sync=false");
    }

    @Test
    @SuppressWarnings("unchecked")
    void registeredConsumerBeanIsBuiltViaSupplier() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        registry.registerSingleton("idempotencyStore", store);

        MessageHandlerRegistry handlerRegistry = mock(MessageHandlerRegistry.class);
        Consumer<Message<?>> stubHandler = msg -> {};
        when(handlerRegistry.getHandler(anyString())).thenReturn(stubHandler);
        registry.registerSingleton("messageHandlerRegistry", handlerRegistry);

        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        registry.registerSingleton("lastProcessedTimestampTracker", tracker);

        registrar().postProcessBeanDefinitionRegistry(registry);

        Consumer<Message<?>> bean = registry.getBean("ordersPrimary", Consumer.class);

        assertThat(bean).isInstanceOf(IdempotentConsumer.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void disabledIdempotencyIgnoresStoreBeanAndProcessesEveryMessage() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.idempotency.enabled", "false");
        loadProperties(props);
        allClustersReachable();

        // A store that marks everything as duplicate — must never be consulted
        IdempotencyStore rejectAll = mock(IdempotencyStore.class);
        when(rejectAll.tryProcess(anyString(), anyString(), any())).thenReturn(false);
        registry.registerSingleton("idempotencyStore", rejectAll);

        MessageHandlerRegistry handlerRegistry = mock(MessageHandlerRegistry.class);
        AtomicInteger processed = new AtomicInteger();
        Consumer<Message<?>> stubHandler = msg -> processed.incrementAndGet();
        when(handlerRegistry.getHandler(anyString())).thenReturn(stubHandler);
        registry.registerSingleton("messageHandlerRegistry", handlerRegistry);
        registry.registerSingleton("lastProcessedTimestampTracker", new LastProcessedTimestampTracker(null));

        registrar().postProcessBeanDefinitionRegistry(registry);

        Consumer<Message<?>> bean = registry.getBean("ordersPrimary", Consumer.class);
        Message<?> msg = MessageBuilder.withPayload("p").build();
        bean.accept(msg);
        bean.accept(msg);

        assertThat(processed.get()).isEqualTo(2);
        verifyNoInteractions(rejectAll);
    }

    @Test
    void initializedClustersPropertyListsReachableOnes() {
        loadProperties(twoClustersWithConsumer());
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("kafka-dr.internal.initialized-clusters"))
                .isEqualTo("primary");
    }

    @Test
    void consumerPropertiesAreSplitBetweenCoreAndKafkaNamespaces() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.properties.ack-mode", "MANUAL_IMMEDIATE");
        props.put("kafka-dr.consumers.orders.properties.concurrency", "3");
        props.put("kafka-dr.consumers.orders.properties.max-attempts", "1");
        props.put("kafka-dr.consumers.orders.properties.configuration.max.poll.records", "500");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        String core = "spring.cloud.stream.bindings.ordersPrimary-in-0.consumer";
        String kafka = "spring.cloud.stream.kafka.bindings.ordersPrimary-in-0.consumer";

        // concurrency and max-attempts used to be written under the Kafka namespace,
        // where the binder ignores them.
        assertThat(environment.getProperty(core + ".concurrency")).isEqualTo("3");
        assertThat(environment.getProperty(core + ".max-attempts")).isEqualTo("1");
        assertThat(environment.getProperty(kafka + ".ack-mode")).isEqualTo("MANUAL_IMMEDIATE");
        assertThat(environment.getProperty(kafka + ".configuration.max.poll.records")).isEqualTo("500");

        assertThat(environment.getProperty(kafka + ".concurrency")).isNull();
        assertThat(environment.getProperty(core + ".ack-mode")).isNull();
    }

    @Test
    void consumerPropertiesAreSplitForEveryCluster() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.properties.concurrency", "2");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        // A setting that applied to one cluster only would change behaviour on failover.
        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.ordersPrimary-in-0.consumer.concurrency")).isEqualTo("2");
        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.ordersSecondary-in-0.consumer.concurrency")).isEqualTo("2");
    }

    @Test
    void producerPropertiesAreSplitBetweenCoreAndKafkaNamespaces() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.events.topic", "events");
        props.put("kafka-dr.producers.events.properties.sync", "true");
        props.put("kafka-dr.producers.events.properties.partition-count", "6");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.events.producer.partition-count")).isEqualTo("6");
        // sync belongs to the Kafka extension; routing it to core would silently
        // turn synchronous sends async and break failover detection.
        assertThat(environment.getProperty(
                "spring.cloud.stream.kafka.bindings.events.producer.sync")).isEqualTo("true");
    }

    @Test
    void starterOwnedConsumerPropertyIsRejected() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.properties.auto-startup", "true");
        loadProperties(props);
        allClustersReachable();

        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("auto-startup");
    }

    @Test
    void batchModeAndClientTuningAreGeneratedForEveryCluster() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.batch.enabled", "true");
        props.put("kafka-dr.consumers.orders.batch.max-records", "500");
        props.put("kafka-dr.consumers.orders.batch.min-bytes", "1024");
        props.put("kafka-dr.consumers.orders.batch.max-wait-ms", "250");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        for (String binding : List.of("ordersPrimary-in-0", "ordersSecondary-in-0")) {
            // batch-mode is a core binding property, not a Kafka extension one
            assertThat(environment.getProperty(
                    "spring.cloud.stream.bindings." + binding + ".consumer.batch-mode")).isEqualTo("true");
            String kafka = "spring.cloud.stream.kafka.bindings." + binding + ".consumer.configuration.";
            assertThat(environment.getProperty(kafka + "max.poll.records")).isEqualTo("500");
            assertThat(environment.getProperty(kafka + "fetch.min.bytes")).isEqualTo("1024");
            assertThat(environment.getProperty(kafka + "fetch.max.wait.ms")).isEqualTo("250");
        }
    }

    @Test
    void batchModeIsAbsentWhenBatchingIsOff() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty(
                "spring.cloud.stream.bindings.ordersPrimary-in-0.consumer.batch-mode")).isNull();
    }

    @Test
    void explicitClientPropertyWinsOverBatchTuning() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.batch.enabled", "true");
        props.put("kafka-dr.consumers.orders.batch.max-records", "500");
        props.put("kafka-dr.consumers.orders.properties.configuration.max.poll.records", "42");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.kafka.bindings.ordersPrimary-in-0"
                + ".consumer.configuration.max.poll.records")).isEqualTo("42");
    }

    @Test
    void registersBatchConsumerBeanWhenBatchingIsOn() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.batch.enabled", "true");
        loadProperties(props);
        allClustersReachable();
        registerConsumerCollaborators();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.getBean("ordersPrimary")).isInstanceOf(
                dev.semeshin.kafkadr.consumer.BatchIdempotentConsumer.class);
    }

    @Test
    void registersPassThroughConsumerBeanForStandardMode() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.batch.enabled", "true");
        props.put("kafka-dr.consumers.orders.batch.mode", "standard");
        props.put("kafka-dr.consumers.orders.content-type", "bytes");
        props.put("kafka-dr.consumers.orders.idempotency-enabled", "false");
        loadProperties(props);
        allClustersReachable();
        registerConsumerCollaborators();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.getBean("ordersPrimary")).isInstanceOf(
                dev.semeshin.kafkadr.consumer.BatchPassThroughConsumer.class);
    }

    @Test
    void standardModeWithIdempotencyStillOnIsRejected() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.batch.enabled", "true");
        props.put("kafka-dr.consumers.orders.batch.mode", "standard");
        props.put("kafka-dr.consumers.orders.content-type", "bytes");
        loadProperties(props);
        allClustersReachable();

        // Silently dropping deduplication in a DR starter is not acceptable.
        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("idempotency-enabled=false");
    }

    @Test
    void perConsumerIdempotencyOverrideDisablesTheStoreForThatConsumerOnly() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers.orders.idempotency-enabled", "false");
        loadProperties(props);
        allClustersReachable();
        registerConsumerCollaborators();

        registrar().postProcessBeanDefinitionRegistry(registry);

        // The store bean is never looked up for this consumer, so a mixed application
        // can keep deduplication on elsewhere.
        assertThat(registry.getBean("ordersPrimary")).isInstanceOf(IdempotentConsumer.class);
    }

    @Test
    void binderTopicAutoCreationIsOffByDefaultOnEveryCluster() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        // The starter provisions topics itself, on every cluster including the standby
        // ones. Leaving the binder's lazy creation on would also drag in
        // KafkaTopicProvisioner, which blocks on metadata against dead brokers.
        for (String cluster : List.of("primary", "secondary")) {
            assertThat(environment.getProperty("spring.cloud.stream.binders." + cluster
                    + ".environment.spring.cloud.stream.kafka.binder.auto-create-topics"))
                    .as(cluster).isEqualTo("false");
        }
    }

    @Test
    void explicitBinderAutoCreationOverridesTheStarterDefault() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.default-environment.spring.cloud.stream.kafka.binder.auto-create-topics", "true");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.primary"
                + ".environment.spring.cloud.stream.kafka.binder.auto-create-topics")).isEqualTo("true");
    }

    @Test
    void perClusterBinderAutoCreationOverridesTheStarterDefault() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.clusters.primary.environment.spring.cloud.stream.kafka.binder.auto-create-topics",
                "true");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.primary"
                + ".environment.spring.cloud.stream.kafka.binder.auto-create-topics")).isEqualTo("true");
        assertThat(environment.getProperty("spring.cloud.stream.binders.secondary"
                + ".environment.spring.cloud.stream.kafka.binder.auto-create-topics")).isEqualTo("false");
    }

    // --- cluster groups -------------------------------------------------------------

    @Test
    void explicitGroupQualifiesBinderBindingAndBeanNames() {
        loadProperties(coreGroupWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.core-primary.type")).isEqualTo("kafka");
        assertThat(environment.getProperty(
                "spring.cloud.stream.binders.core-secondary.environment.spring.cloud.stream.kafka.binder.brokers"))
                .isEqualTo("core-b:9092");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersCorePrimary-in-0.binder"))
                .isEqualTo("core-primary");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersCoreSecondary-in-0.destination"))
                .isEqualTo("orders");
        // Map binding from a HashMap-backed source does not preserve order.
        assertThat(environment.getProperty("spring.cloud.function.definition").split(";"))
                .containsExactlyInAnyOrder("ordersCorePrimary", "ordersCoreSecondary");
        assertThat(registry.containsBeanDefinition("ordersCorePrimary")).isTrue();
        assertThat(registry.containsBeanDefinition("ordersCoreSecondary")).isTrue();
        assertThat(environment.getProperty("kafka-dr.internal.initialized-clusters").split(","))
                .containsExactlyInAnyOrder("core-primary", "core-secondary");
    }

    @Test
    void groupEnvironmentSitsBetweenGlobalAndClusterEnvironment() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        String key = "spring.cloud.stream.kafka.binder.configuration.schema.registry.url";
        props.put("kafka-dr.default-environment." + key, "http://global-sr:8081");
        props.put("kafka-dr.cluster-groups.core.default-environment." + key, "http://core-sr:8081");
        props.put("kafka-dr.cluster-groups.core.clusters.primary.environment." + key, "http://core-a-sr:8081");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.core-primary.environment." + key))
                .isEqualTo("http://core-a-sr:8081");
        assertThat(environment.getProperty("spring.cloud.stream.binders.core-secondary.environment." + key))
                .isEqualTo("http://core-sr:8081");
    }

    @Test
    void groupAutoCreateTopicsProvisionsTheGroupClusters() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        props.put("kafka-dr.cluster-groups.core.auto-create-topics", "true");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("core-primary"), eq("core-a:9092"),
                any(KafkaClusterProperties.class), any(ClusterTopology.class)));
        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("core-secondary"), eq("core-b:9092"),
                any(KafkaClusterProperties.class), any(ClusterTopology.class)));
    }

    @Test
    void severalGroupsBindEachConsumerOnlyToTheClustersOfItsGroup() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        props.put("kafka-dr.consumers.orders.cluster-group", "core");
        props.put("kafka-dr.cluster-groups.analytics.clusters.dc1.bootstrap-servers", "an-a:9092");
        props.put("kafka-dr.consumers.scores.topic", "scores");
        props.put("kafka-dr.consumers.scores.handler", "processScore");
        props.put("kafka-dr.consumers.scores.cluster-group", "analytics");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.binders.analytics-dc1.type")).isEqualTo("kafka");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.scoresAnalyticsDc1-in-0.binder"))
                .isEqualTo("analytics-dc1");
        // Each consumer is bound in its own Kafka only.
        assertThat(environment.getProperty("spring.cloud.stream.bindings.ordersAnalyticsDc1-in-0.destination")).isNull();
        assertThat(environment.getProperty("spring.cloud.stream.bindings.scoresCorePrimary-in-0.destination")).isNull();
        assertThat(environment.getProperty("spring.cloud.function.definition").split(";"))
                .containsExactlyInAnyOrder("ordersCorePrimary", "ordersCoreSecondary", "scoresAnalyticsDc1");
        assertThat(registry.containsBeanDefinition("scoresAnalyticsDc1")).isTrue();
        assertThat(registry.containsBeanDefinition("scoresCorePrimary")).isFalse();
    }

    @Test
    void severalGroupsStillReportConfigurationErrorsFirst() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        props.put("kafka-dr.cluster-groups.analytics.clusters.dc1.bootstrap-servers", "an-a:9092");
        loadProperties(props);

        // The consumer has no cluster-group: that is the error to fix, not the group count.
        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.consumers.orders has no cluster-group");
    }

    @Test
    void sharedBrokersAreRejectedBeforeAnyProbe() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.clusters.secondary.bootstrap-servers", "kafka-primary:9092");
        loadProperties(props);

        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broker 'kafka-primary:9092'");
        staticHelper.verifyNoInteractions();
    }

    @Test
    @SuppressWarnings("unchecked")
    void consumerWithDependsOnIsBuiltWithTheGuardsGate() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        props.put("kafka-dr.consumers.orders.cluster-group", "core");
        props.put("kafka-dr.consumers.orders.depends-on", "analytics");
        props.put("kafka-dr.consumers.orders.properties.ack-mode", "MANUAL");
        props.put("kafka-dr.cluster-groups.analytics.clusters.dc1.bootstrap-servers", "an-a:9092");
        loadProperties(props);
        allClustersReachable();
        registerConsumerCollaborators();
        DependencyGuard guard = mock(DependencyGuard.class);
        when(guard.gateFor("orders")).thenReturn(DependencyGate.NONE);
        registry.registerSingleton("dependencyGuard", guard);

        registrar().postProcessBeanDefinitionRegistry(registry);
        registry.getBean("ordersCorePrimary", Consumer.class);

        verify(guard).gateFor("orders");
    }

    @Test
    void dependsOnWithoutManualAckModeStopsTheStartup() {
        Map<String, String> props = new HashMap<>(coreGroupWithConsumer());
        props.put("kafka-dr.consumers.orders.cluster-group", "core");
        props.put("kafka-dr.consumers.orders.depends-on", "analytics");
        props.put("kafka-dr.cluster-groups.analytics.clusters.dc1.bootstrap-servers", "an-a:9092");
        loadProperties(props);

        assertThatThrownBy(() -> registrar().postProcessBeanDefinitionRegistry(registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("needs ack-mode=MANUAL or MANUAL_IMMEDIATE");
    }

    @Test
    void producerChannelCacheIsSizedForEveryProducerOnEveryClusterOfItsGroup() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        for (int i = 0; i < 6; i++) {
            props.put("kafka-dr.producers.p" + i + ".topic", "t" + i);
        }
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        // 6 producers × 2 clusters: the default of 10 would unbind producers still in use.
        assertThat(environment.getProperty("spring.cloud.stream.dynamic-destination-cache-size")).isEqualTo("12");
    }

    @Test
    void explicitProducerChannelCacheSizeIsKept() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers.p.topic", "t");
        props.put("spring.cloud.stream.dynamic-destination-cache-size", "50");
        loadProperties(props);
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.dynamic-destination-cache-size")).isEqualTo("50");
    }

    @Test
    void smallConfigurationsKeepTheSpringCloudStreamDefault() {
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(environment.getProperty("spring.cloud.stream.dynamic-destination-cache-size")).isEqualTo("10");
    }

    private static Map<String, String> coreGroupWithConsumer() {
        Map<String, String> props = new HashMap<>();
        props.put("kafka-dr.enabled", "true");
        props.put("kafka-dr.cluster-groups.core.clusters.primary.bootstrap-servers", "core-a:9092");
        props.put("kafka-dr.cluster-groups.core.clusters.primary.priority", "1");
        props.put("kafka-dr.cluster-groups.core.clusters.secondary.bootstrap-servers", "core-b:9092");
        props.put("kafka-dr.cluster-groups.core.clusters.secondary.priority", "2");
        props.put("kafka-dr.consumers.orders.topic", "orders");
        props.put("kafka-dr.consumers.orders.handler", "processOrder");
        props.put("kafka-dr.consumers.orders.group", "dr-group");
        return props;
    }

    /** Beans the generated consumer function beans resolve lazily from the factory. */
    @SuppressWarnings("unchecked")
    private void registerConsumerCollaborators() {
        registry.registerSingleton("idempotencyStore", new InMemoryIdempotencyStore());
        MessageHandlerRegistry handlerRegistry = mock(MessageHandlerRegistry.class);
        when(handlerRegistry.getHandler(anyString())).thenReturn((Consumer<Message<?>>) msg -> {});
        registry.registerSingleton("messageHandlerRegistry", handlerRegistry);
        registry.registerSingleton("lastProcessedTimestampTracker", new LastProcessedTimestampTracker(null));
    }

    private DynamicBindingRegistrar registrar() {
        DynamicBindingRegistrar r = new DynamicBindingRegistrar();
        r.setEnvironment(environment);
        return r;
    }

    private void loadProperties(Map<String, String> props) {
        environment.getPropertySources()
                .addFirst(new MapPropertySource("test", new HashMap<>(props)));
    }

    private void allClustersReachable() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(anyString(), any())).thenReturn(true);
    }

    private static Map<String, String> twoClustersWithConsumer() {
        Map<String, String> props = new HashMap<>();
        props.put("kafka-dr.enabled", "true");
        props.put("kafka-dr.clusters.primary.bootstrap-servers", "kafka-primary:9092");
        props.put("kafka-dr.clusters.primary.priority", "1");
        props.put("kafka-dr.clusters.secondary.bootstrap-servers", "kafka-secondary:9092");
        props.put("kafka-dr.clusters.secondary.priority", "2");
        props.put("kafka-dr.consumers.orders.topic", "orders");
        props.put("kafka-dr.consumers.orders.handler", "processOrder");
        props.put("kafka-dr.consumers.orders.group", "dr-group");
        return props;
    }

}
