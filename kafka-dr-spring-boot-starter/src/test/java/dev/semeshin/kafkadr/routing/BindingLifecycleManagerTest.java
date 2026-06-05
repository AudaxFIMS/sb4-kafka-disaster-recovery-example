package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.StartupClusterState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.cloud.stream.function.StreamBridge;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class BindingLifecycleManagerTest {

    private BindingsLifecycleController bindingsController;
    private StreamBridge streamBridge;
    private StartupClusterState startupState;
    private LateBindingInitializer lateInit;
    private KafkaClusterProperties properties;

    @BeforeEach
    void setup() {
        bindingsController = mock(BindingsLifecycleController.class);
        streamBridge = mock(StreamBridge.class);
        startupState = new StartupClusterState();
        lateInit = mock(LateBindingInitializer.class);
        properties = twoClusterPropertiesWithConsumer();
    }

    @Test
    void onSwitchStopsPreviousAndStartsNextForStartupClusters() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);
        verify(bindingsController).changeState("ordersSecondary-in-0", State.STARTED);
    }

    @Test
    void routesToLateBindingInitializerForRuntimeInitializedClusters() {
        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        startupState.addInitializedCluster("secondary");

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(lateInit).startBindings("secondary");
        verify(bindingsController, never()).changeState(anyString(), any());
    }

    @Test
    void stopsLateBindingsWhenPreviousClusterWasLateInitialized() {
        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(lateInit).stopBindings("primary");
        verify(lateInit).startBindings("secondary");
    }

    @Test
    void skipsStartWhenNextClusterNotYetInitialized() {
        startupState.addInitializedCluster("primary");

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);
        verify(bindingsController, never()).changeState("ordersSecondary-in-0", State.STARTED);
        verify(lateInit, never()).startBindings(anyString());
    }

    @Test
    void clearsProducerCacheEntriesForPreviousCluster() throws Exception {
        Map<String, Object> cache = new HashMap<>();
        cache.put("order-events:primary", new Object());
        cache.put("event:primary", new Object());
        cache.put("order-events:secondary", new Object());
        cache.put("unrelated", new Object());

        StreamBridge bridge = realStreamBridgeWithCache(cache);

        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, bridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        assertThat(cache)
                .doesNotContainKey("order-events:primary")
                .doesNotContainKey("event:primary")
                .containsKey("order-events:secondary")
                .containsKey("unrelated");
    }

    @Test
    void cacheCleanupHandlesEmptyOrAbsentCacheGracefully() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);
    }

    @Test
    void failureToStopBindingDoesNotPreventStart() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        doThrow(new RuntimeException("stop failed"))
                .when(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersSecondary-in-0", State.STARTED);
    }

    @Test
    void failureToStartBindingIsSwallowed() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        doThrow(new RuntimeException("start failed"))
                .when(bindingsController).changeState("ordersSecondary-in-0", State.STARTED);

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, streamBridge, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));
    }

    private static StreamBridge realStreamBridgeWithCache(Map<String, Object> cache) throws Exception {
        StreamBridge bridge = mock(StreamBridge.class);
        Field f = StreamBridge.class.getDeclaredField("channelCache");
        f.setAccessible(true);
        f.set(bridge, cache);
        return bridge;
    }

    private static KafkaClusterProperties twoClusterPropertiesWithConsumer() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        ClusterConfig primary = new ClusterConfig();
        primary.setBootstrapServers("kafka-primary:9092");
        ClusterConfig secondary = new ClusterConfig();
        secondary.setBootstrapServers("kafka-secondary:9092");
        clusters.put("primary", primary);
        clusters.put("secondary", secondary);
        props.setClusters(clusters);

        ConsumerConfig consumer = new ConsumerConfig();
        consumer.setTopic("orders");
        consumer.setGroup("dr-group");
        consumer.setHandler("processOrder");
        props.setConsumers(Map.of("orders", consumer));
        return props;
    }
}
