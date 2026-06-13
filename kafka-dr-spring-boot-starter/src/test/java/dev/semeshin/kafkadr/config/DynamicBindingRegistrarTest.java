package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.consumer.IdempotentConsumer;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
        staticHelper.when(() -> KafkaAdminHelper.provisionTopics(anyString(), anyString(), any()))
                .thenAnswer(inv -> null);

        registrar().postProcessBeanDefinitionRegistry(registry);

        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("primary"), eq("kafka-primary:9092"), any()));
        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("secondary"), eq("kafka-secondary:9092"), any()));
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
