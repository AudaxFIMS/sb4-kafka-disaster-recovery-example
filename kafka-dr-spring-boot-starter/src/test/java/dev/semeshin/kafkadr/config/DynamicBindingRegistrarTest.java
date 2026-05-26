package dev.semeshin.kafkadr.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

class DynamicBindingRegistrarTest {

    private DefaultListableBeanFactory registry;
    private StandardEnvironment environment;
    private MockedStatic<KafkaAdminHelper> staticHelper;

    @BeforeEach
    void setup() {
        registry = new DefaultListableBeanFactory();
        registerStubBeans();
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
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("secondary"), any())).thenReturn(false);

        registrar().postProcessBeanDefinitionRegistry(registry);

        String defs = environment.getProperty("spring.cloud.function.definition");
        assertThat(defs).isEqualTo("ordersPrimary");
    }

    @Test
    void registersFunctionBeansForAllClustersIncludingUnreachable() {
        loadProperties(twoClustersWithConsumer());
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("secondary"), any())).thenReturn(false);

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.containsBeanDefinition("ordersPrimary")).isTrue();
        assertThat(registry.containsBeanDefinition("ordersSecondary")).isTrue();
    }

    @Test
    void producerBindingsAreGeneratedForBothBaseAndOutZero() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.producers[0].topic", "events");
        props.put("kafka-dr.producers[0].content-type", "json");
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
        registry.registerBeanDefinition("kafkaAdmin",
                new org.springframework.beans.factory.support.GenericBeanDefinition() {{
                    setBeanClassName("org.springframework.kafka.core.KafkaAdmin");
                }});
        loadProperties(twoClustersWithConsumer());
        allClustersReachable();

        registrar().postProcessBeanDefinitionRegistry(registry);

        assertThat(registry.containsBeanDefinition("kafkaAdmin")).isFalse();
    }

    @Test
    void consumerConfigurationPropertiesArePassedToBinding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers[0].properties.configuration.value.deserializer",
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
        props.put("kafka-dr.producers[0].topic", "events");
        props.put("kafka-dr.producers[0].properties.configuration.acks", "1");
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

        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eqArg("primary"), eqArg("kafka-primary:9092"), any()));
        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eqArg("secondary"), eqArg("kafka-secondary:9092"), any()));
    }

    @Test
    void nativeConsumerContentTypeEnablesNativeDecoding() {
        Map<String, String> props = new HashMap<>(twoClustersWithConsumer());
        props.put("kafka-dr.consumers[0].content-type", "native");
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
        props.put("kafka-dr.producers[0].topic", "events");
        props.put("kafka-dr.producers[0].content-type", "native");
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

        dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore store =
                new dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore();
        registry.registerSingleton("idempotencyStore", store);

        dev.semeshin.kafkadr.consumer.MessageHandlerRegistry handlerRegistry =
                org.mockito.Mockito.mock(dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.class);
        java.util.function.Consumer<org.springframework.messaging.Message<?>> stubHandler = msg -> {};
        org.mockito.Mockito.when(handlerRegistry.getHandler(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(stubHandler);
        registry.registerSingleton("messageHandlerRegistry", handlerRegistry);

        dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker tracker =
                new dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker(null);
        registry.registerSingleton("lastProcessedTimestampTracker", tracker);

        registrar().postProcessBeanDefinitionRegistry(registry);

        java.util.function.Consumer<org.springframework.messaging.Message<?>> bean =
                registry.getBean("ordersPrimary", java.util.function.Consumer.class);

        assertThat(bean).isInstanceOf(dev.semeshin.kafkadr.consumer.IdempotentConsumer.class);
    }

    @Test
    void initializedClustersPropertyListsReachableOnes() {
        loadProperties(twoClustersWithConsumer());
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eqArg("secondary"), any())).thenReturn(false);

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

    private static String eqArg(String s) {
        return org.mockito.ArgumentMatchers.eq(s);
    }

    private static Map<String, String> twoClustersWithConsumer() {
        Map<String, String> props = new HashMap<>();
        props.put("kafka-dr.enabled", "true");
        props.put("kafka-dr.clusters.primary.bootstrap-servers", "kafka-primary:9092");
        props.put("kafka-dr.clusters.primary.priority", "1");
        props.put("kafka-dr.clusters.secondary.bootstrap-servers", "kafka-secondary:9092");
        props.put("kafka-dr.clusters.secondary.priority", "2");
        props.put("kafka-dr.consumers[0].topic", "orders");
        props.put("kafka-dr.consumers[0].handler", "processOrder");
        props.put("kafka-dr.consumers[0].group", "dr-group");
        return props;
    }

    private void registerStubBeans() {
        // IdempotentConsumer instance supplier looks up these beans on first use,
        // not during registration. Tests don't invoke the consumers, so no stubs needed.
    }
}
