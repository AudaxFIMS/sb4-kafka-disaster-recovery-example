package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.StartupClusterState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;

import java.util.LinkedHashMap;
import java.util.List;
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
    private StartupClusterState startupState;
    private LateBindingInitializer lateInit;
    private KafkaClusterProperties properties;

    @BeforeEach
    void setup() {
        bindingsController = mock(BindingsLifecycleController.class);
        startupState = new StartupClusterState();
        lateInit = mock(LateBindingInitializer.class);
        properties = twoClusterPropertiesWithConsumer();
    }

    @Test
    void onSwitchStopsPreviousAndStartsNextForStartupClusters() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);
        verify(bindingsController).changeState("ordersSecondary-in-0", State.STARTED);
    }

    @Test
    void bindingCreatedLateOnTheActiveClusterIsStarted() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");
        ActiveClusterManager clusters = mock(ActiveClusterManager.class);
        org.mockito.Mockito.when(clusters.hasHealthyCluster("default")).thenReturn(true);
        org.mockito.Mockito.when(clusters.getActiveCluster("default")).thenReturn("primary");
        BindingLifecycleManager manager = managerWith(clusters);

        // Its topic was missing at startup; the binding service retried until it appeared.
        org.springframework.cloud.stream.binder.Binding<?> late = binding("ordersPrimary-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(late));

        verify(late).start();
    }

    @Test
    void bindingCreatedLateIsLeftAloneUnlessItsClusterIsActiveAndElected() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");
        ActiveClusterManager clusters = mock(ActiveClusterManager.class);
        org.mockito.Mockito.when(clusters.getActiveCluster("default")).thenReturn("primary");
        BindingLifecycleManager manager = managerWith(clusters);

        // Before the initial election: the election's switch starts it.
        org.springframework.cloud.stream.binder.Binding<?> beforeElection = binding("ordersPrimary-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(beforeElection));
        verify(beforeElection, never()).start();

        org.mockito.Mockito.when(clusters.hasHealthyCluster("default")).thenReturn(true);
        // The standby: started by the switch that makes it active, if one ever does.
        org.springframework.cloud.stream.binder.Binding<?> standby = binding("ordersSecondary-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(standby));
        verify(standby, never()).start();

        // A producer binding, and one that is not a DR consumer's.
        org.springframework.cloud.stream.binder.Binding<?> output = binding("ordersPrimary-in-0", false);
        org.springframework.cloud.stream.binder.Binding<?> foreign = binding("somethingElse-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(output));
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(foreign));
        verify(output, never()).start();
        verify(foreign, never()).start();
    }

    @Test
    void bindingCreatedLateOnAStandbyIsStartedByTheFailoverToIt() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");
        ActiveClusterManager clusters = mock(ActiveClusterManager.class);
        org.mockito.Mockito.when(clusters.hasHealthyCluster("default")).thenReturn(true);
        org.mockito.Mockito.when(clusters.getActiveCluster("default")).thenReturn("primary");
        BindingLifecycleManager manager = managerWith(clusters);
        // The binding service holds only a placeholder, named after the topic: the controller
        // finds nothing under the binding's name (the mock's default).
        org.springframework.cloud.stream.binder.Binding<?> standby = binding("ordersSecondary-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(standby));
        verify(standby, never()).start();

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(standby).start();
    }

    @Test
    void bindingCreatedLateIsStoppedAndPausedLikeAnyOther() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");
        ActiveClusterManager clusters = mock(ActiveClusterManager.class);
        org.mockito.Mockito.when(clusters.hasHealthyCluster("default")).thenReturn(true);
        org.mockito.Mockito.when(clusters.getActiveCluster("default")).thenReturn("primary");
        BindingLifecycleManager manager = managerWith(clusters);
        org.springframework.cloud.stream.binder.Binding<?> late = binding("ordersPrimary-in-0", true);
        manager.onBindingCreated(new org.springframework.cloud.stream.binder.BindingCreatedEvent(late));
        verify(late).start();

        // depends-on pausing it, then a failover away from its cluster.
        assertThat(manager.pauseConsumer("primary", "orders")).isTrue();
        verify(late).pause();
        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        // Left running, it would keep reading the cluster the group just left.
        verify(late).stop();
    }

    private BindingLifecycleManager managerWith(ActiveClusterManager clusters) {
        @SuppressWarnings("unchecked")
        org.springframework.beans.factory.ObjectProvider<ActiveClusterManager> provider =
                mock(org.springframework.beans.factory.ObjectProvider.class);
        org.mockito.Mockito.when(provider.getIfAvailable()).thenReturn(clusters);
        return new BindingLifecycleManager(bindingsController, properties, properties.topology(), startupState,
                lateInit, provider);
    }

    private static org.springframework.cloud.stream.binder.Binding<?> binding(String name, boolean input) {
        org.springframework.cloud.stream.binder.Binding<?> binding =
                mock(org.springframework.cloud.stream.binder.Binding.class);
        org.mockito.Mockito.when(binding.getBindingName()).thenReturn(name);
        org.mockito.Mockito.when(binding.isInput()).thenReturn(input);
        return binding;
    }

    @Test
    void routesToLateBindingInitializerForRuntimeInitializedClusters() {
        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, properties, startupState, lateInit);

        startupState.addInitializedCluster("secondary");

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(lateInit).startBindings("secondary");
        verify(bindingsController, never()).changeState(anyString(), any());
    }

    @Test
    void stopsLateBindingsWhenPreviousClusterWasLateInitialized() {
        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, properties, startupState, lateInit);

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
                bindingsController, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));

        verify(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);
        verify(bindingsController, never()).changeState("ordersSecondary-in-0", State.STARTED);
        verify(lateInit, never()).startBindings(anyString());
    }

    @Test
    void failureToStopBindingDoesNotPreventStart() {
        startupState.addInitializedCluster("primary");
        startupState.addInitializedCluster("secondary");

        doThrow(new RuntimeException("stop failed"))
                .when(bindingsController).changeState("ordersPrimary-in-0", State.STOPPED);

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, properties, startupState, lateInit);

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
                bindingsController, properties, startupState, lateInit);

        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "primary", "secondary"));
    }

    @Test
    void switchInOneClusterGroupLeavesTheOtherGroupBindingsAlone() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", clusterGroup("primary", "core-a:9092", "secondary", "core-b:9092"));
        groups.put("analytics", clusterGroup("dc1", "an-a:9092", "dc2", "an-b:9092"));
        props.setClusterGroups(groups);
        ConsumerConfig orders = new ConsumerConfig();
        orders.setTopic("orders");
        orders.setClusterGroup("core");
        ConsumerConfig scores = new ConsumerConfig();
        scores.setTopic("scores");
        scores.setClusterGroup("analytics");
        props.setConsumers(Map.of("orders", orders, "scores", scores));
        for (String id : List.of("core-primary", "core-secondary", "analytics-dc1", "analytics-dc2")) {
            startupState.addInitializedCluster(id);
        }

        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, props, startupState, lateInit);
        manager.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-secondary"));

        verify(bindingsController).changeState("ordersCorePrimary-in-0", State.STOPPED);
        verify(bindingsController).changeState("ordersCoreSecondary-in-0", State.STARTED);
        verify(bindingsController, never()).changeState(org.mockito.ArgumentMatchers.startsWith("scores"),
                org.mockito.ArgumentMatchers.any());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void pauseOfABindingTheControllerDoesNotKnowIsNotReportedAsDone() {
        startupState.addInitializedCluster("primary");
        BindingLifecycleManager manager = new BindingLifecycleManager(
                bindingsController, properties, startupState, lateInit);

        // changeState would silently do nothing for an unknown name.
        assertThat(manager.pauseConsumer("primary", "orders")).isFalse();
        verify(bindingsController, never()).changeState("ordersPrimary-in-0", State.PAUSED);

        org.mockito.Mockito.when(bindingsController.queryState("ordersPrimary-in-0"))
                .thenReturn(List.of(mock(org.springframework.cloud.stream.binder.Binding.class)));
        assertThat(manager.pauseConsumer("primary", "orders")).isTrue();
        verify(bindingsController).changeState("ordersPrimary-in-0", State.PAUSED);
    }

    private static KafkaClusterProperties.ClusterGroupConfig clusterGroup(String a, String brokersA,
                                                                          String b, String brokersB) {
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        ClusterConfig first = new ClusterConfig();
        first.setBootstrapServers(brokersA);
        ClusterConfig second = new ClusterConfig();
        second.setBootstrapServers(brokersB);
        clusters.put(a, first);
        clusters.put(b, second);
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        group.setClusters(clusters);
        return group;
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
