package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.StartupClusterState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LateBindingInitializerTest {

    private KafkaClusterProperties properties;
    private StartupClusterState startupState;
    private ActiveClusterManager clusterManager;
    private BinderFactory binderFactory;
    private BindingServiceProperties bindingServiceProperties;
    private ApplicationContext applicationContext;
    private Binder<MessageChannel, ExtendedConsumerProperties<KafkaConsumerProperties>, ?> binder;
    private Binding<?> binding;
    private MockedStatic<KafkaAdminHelper> staticHelper;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        properties = twoClusterProperties();
        startupState = new StartupClusterState();
        clusterManager = mock(ActiveClusterManager.class);
        binderFactory = mock(BinderFactory.class);
        bindingServiceProperties = mock(BindingServiceProperties.class);
        applicationContext = mock(ApplicationContext.class);
        binder = mock(Binder.class);
        binding = mock(Binding.class);

        when(binderFactory.getBinder(anyString(), eq(MessageChannel.class))).thenReturn((Binder) binder);

        BindingProperties bp = new BindingProperties();
        bp.setDestination("orders");
        bp.setGroup("dr-group");
        when(bindingServiceProperties.getBindingProperties(anyString())).thenReturn(bp);

        Consumer<Message<?>> stubBean = msg -> {};
        when(applicationContext.getBean(anyString(), eq(Consumer.class))).thenReturn(stubBean);

        when(binder.bindConsumer(anyString(), anyString(), any(MessageChannel.class), any()))
                .thenReturn((Binding) binding);

        staticHelper = mockStatic(KafkaAdminHelper.class);
    }

    @AfterEach
    void tearDown() {
        staticHelper.close();
    }

    @Test
    void skipsAlreadyInitializedClusters() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        newInitializer().checkAndInitializeClusters();

        verify(binderFactory, never()).getBinder(anyString(), any());
    }

    @Test
    void skipsUnreachableClusters() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(anyString(), any()))
                .thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        verify(binderFactory, never()).getBinder(anyString(), any());
        assertThat(startupState.getInitializedClusters()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void createsBindingsForNewlyReachableCluster() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        verify(binderFactory).getBinder(eq("primary"), eq(MessageChannel.class));
        verify(binder).bindConsumer(eq("orders"), eq("dr-group"),
                any(MessageChannel.class), any(ExtendedConsumerProperties.class));
        assertThat(startupState.isInitialized("primary")).isTrue();
        assertThat(startupState.isInitialized("secondary")).isFalse();
    }

    @Test
    void provisionsTopicsWhenAutoCreateEnabled() {
        properties.setAutoCreateTopics(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        staticHelper.verify(() -> KafkaAdminHelper.provisionTopics(eq("primary"),
                eq("kafka-primary:9092"), any(KafkaClusterProperties.class)));
    }

    @Test
    void startsBindingsImmediatelyWhenClusterAlreadyActive() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(false);
        when(clusterManager.getActiveCluster()).thenReturn("secondary");

        newInitializer().checkAndInitializeClusters();

        verify(binding, atLeastOnce()).start();
    }

    @Test
    void doesNotStartBindingsWhenClusterNotActive() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);
        when(clusterManager.getActiveCluster()).thenReturn("secondary");

        newInitializer().checkAndInitializeClusters();

        verify(binding, never()).start();
    }

    @Test
    void startBindingsTogglesEachLateBindingForCluster() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        LateBindingInitializer init = newInitializer();
        init.checkAndInitializeClusters();

        init.startBindings("primary");
        verify(binding, atLeastOnce()).start();
    }

    @Test
    void stopBindingsTogglesEachLateBindingForCluster() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        LateBindingInitializer init = newInitializer();
        init.checkAndInitializeClusters();

        init.stopBindings("primary");
        verify(binding).stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    void handlerExceptionsAreSwallowedByChannelSubscriber() {
        Consumer<Message<?>> failingHandler = msg -> {
            throw new RuntimeException("downstream failure");
        };
        when(applicationContext.getBean(anyString(), eq(Consumer.class)))
                .thenReturn(failingHandler);

        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        ArgumentCaptor<MessageChannel> channelCaptor =
                ArgumentCaptor.forClass(MessageChannel.class);

        newInitializer().checkAndInitializeClusters();

        verify(binder).bindConsumer(anyString(), anyString(),
                channelCaptor.capture(), any(ExtendedConsumerProperties.class));

        MessageChannel channel = channelCaptor.getValue();
        boolean sent = channel.send(MessageBuilder.withPayload("payload").build());
        assertThat(sent).isTrue();
    }

    @Test
    void nativeContentTypeEnablesNativeDecoding() {
        properties.getConsumers().get("orders").setContentType("native");
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ExtendedConsumerProperties<KafkaConsumerProperties>> captor =
                ArgumentCaptor.forClass(ExtendedConsumerProperties.class);
        verify(binder).bindConsumer(anyString(), anyString(), any(MessageChannel.class), captor.capture());
        assertThat(captor.getValue().isUseNativeDecoding()).isTrue();
    }

    @Test
    void appliesConsumerConfigurationProperties() {
        properties.getConsumers().get("orders").setProperties(Map.of(
                "configuration", Map.of("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
        ));
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ExtendedConsumerProperties<KafkaConsumerProperties>> captor =
                ArgumentCaptor.forClass(ExtendedConsumerProperties.class);
        verify(binder).bindConsumer(anyString(), anyString(), any(MessageChannel.class), captor.capture());
        assertThat(captor.getValue().getExtension().getConfiguration())
                .containsEntry("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    }

    @Test
    void usesFallbackGroupAndDestinationWhenBindingPropertiesIncomplete() {
        BindingProperties empty = new BindingProperties();
        when(bindingServiceProperties.getBindingProperties(anyString())).thenReturn(empty);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        verify(binder).bindConsumer(eq("orders"), eq("dr-group"),
                any(MessageChannel.class), any(ExtendedConsumerProperties.class));
    }

    @Test
    void missingFunctionBeanSkipsBindingCreation() {
        when(applicationContext.getBean(anyString(), eq(Consumer.class)))
                .thenThrow(new RuntimeException("bean missing"));
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("secondary"), any())).thenReturn(false);

        newInitializer().checkAndInitializeClusters();

        verify(binder, never()).bindConsumer(anyString(), anyString(), any(MessageChannel.class), any());
        assertThat(startupState.isInitialized("primary")).isTrue();
    }

    @Test
    void initializationFailureLeavesClusterUninitialized() {
        staticHelper.when(() -> KafkaAdminHelper.probeCluster(eq("primary"), any())).thenReturn(true);
        when(binder.bindConsumer(anyString(), anyString(), any(MessageChannel.class), any()))
                .thenThrow(new RuntimeException("boom"));

        newInitializer().checkAndInitializeClusters();

        assertThat(startupState.isInitialized("primary")).isFalse();
    }

    private LateBindingInitializer newInitializer() {
        return new LateBindingInitializer(properties, startupState, clusterManager,
                binderFactory, bindingServiceProperties, applicationContext);
    }

    private static KafkaClusterProperties twoClusterProperties() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("primary", clusterCfg("kafka-primary:9092", 1));
        clusters.put("secondary", clusterCfg("kafka-secondary:9092", 2));
        props.setClusters(clusters);

        ConsumerConfig consumer = new ConsumerConfig();
        consumer.setTopic("orders");
        consumer.setGroup("dr-group");
        consumer.setHandler("processOrder");
        props.setConsumers(Map.of("orders", consumer));
        return props;
    }

    private static ClusterConfig clusterCfg(String brokers, int priority) {
        ClusterConfig c = new ClusterConfig();
        c.setBootstrapServers(brokers);
        c.setPriority(priority);
        return c;
    }
}
