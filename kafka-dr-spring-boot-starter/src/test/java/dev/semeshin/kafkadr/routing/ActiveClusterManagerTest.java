package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.routing.FailoverStateStore.FailoverState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActiveClusterManagerTest {

    private List<ApplicationEvent> events;
    private ApplicationEventPublisher publisher;

    @BeforeEach
    void setup() {
        events = new ArrayList<>();
        publisher = event -> {
            if (event instanceof ApplicationEvent ae) {
                events.add(ae);
            }
        };
    }

    @Test
    void emptyClustersFailsAtConstruction() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        assertThatThrownBy(() -> new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void initialActiveIsFirstByPriority() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(mgr.getClustersByPriority()).containsExactly("primary", "secondary", "tertiary");
    }

    @Test
    void firstHealthyClusterIsElectedImmediatelyOnInitialReport() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(mgr.getHealthStatuses()).containsEntry("primary", true);
    }

    @Test
    void failoverWhenActiveClusterCrossesFailureThreshold() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        events.clear();

        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);

        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(events).hasSize(1);
        ClusterSwitchedEvent evt = (ClusterSwitchedEvent) events.get(0);
        assertThat(evt.getPreviousCluster()).isEqualTo("primary");
        assertThat(evt.getNewCluster()).isEqualTo("secondary");
    }

    @Test
    void failbackToHigherPriorityWhenRecoveryThresholdMet() {
        KafkaClusterProperties props = threeClusters();
        FailoverStateStore store = new InMemoryFailoverStateStore();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);

        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");

        mgr.reportHealth("primary", true);
        mgr.reportHealth("primary", true);
        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void forceUnhealthyTriggersImmediateReelection() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        events.clear();

        mgr.forceUnhealthy("primary");

        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(events).hasSize(1);
    }

    @Test
    void failbackAfterGateBlocksFailbackUntilThreshold() {
        KafkaClusterProperties props = threeClusters();
        LocalTime future = LocalTime.now().plusHours(2)
                .withMinute(0).withSecond(0).withNano(0);
        props.getFailover().setFailbackAfter(future.toString());

        FailoverStateStore store = new InMemoryFailoverStateStore();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("secondary", true);

        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);
        mgr.reportHealth("primary", false);
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(store.load()).isPresent();

        mgr.reportHealth("primary", true);
        mgr.reportHealth("primary", true);
        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(store.load()).isPresent();
    }

    @Test
    void restoresActiveClusterFromStoreWhenThresholdNotPassed() {
        KafkaClusterProperties props = threeClusters();
        LocalTime future = LocalTime.now().plusHours(2)
                .withMinute(0).withSecond(0).withNano(0);
        props.getFailover().setFailbackAfter(future.toString());

        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now().minusSeconds(60)));

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void clearsStoreWhenPersistedFailoverIsPastThreshold() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("12:00:00");

        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now().minusSeconds(86400 * 2)));

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void failbackThresholdIsBumpedToNextDayWhenFailoverWasAfterFailbackTime() {
        KafkaClusterProperties props = threeClusters();
        LocalTime earlyTime = LocalTime.of(2, 0, 0);
        props.getFailover().setFailbackAfter(earlyTime.toString());

        ZonedDateTime nowSystem = ZonedDateTime.now();
        ZonedDateTime nightTime = nowSystem.with(LocalTime.of(23, 0, 0)).minusDays(1);
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", nightTime.toInstant()));

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
    }

    @Test
    void isFailbackBlockedReturnsFalseWhenFailoverAtIsCleared() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("23:59:59");

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
    }

    @Test
    void clearsStoreWhenPersistedClusterNotInConfig() {
        KafkaClusterProperties props = threeClusters();
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("ghost-cluster", Instant.now()));

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void clearsStoreWhenFailbackAfterNotConfigured() {
        KafkaClusterProperties props = threeClusters();
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now()));

        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void forceUnhealthyOnAlreadyUnhealthyClusterIsNoOp() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("secondary", true);
        events.clear();

        mgr.forceUnhealthy("primary");

        assertThat(events).isEmpty();
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void allClustersUnhealthyDoesNotMutateActive() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = new ActiveClusterManager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", false);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
    }

    private static KafkaClusterProperties threeClusters() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("primary", clusterWith(1));
        clusters.put("secondary", clusterWith(2));
        clusters.put("tertiary", clusterWith(3));
        props.setClusters(clusters);
        props.getHealthCheck().setFailureThreshold(3);
        props.getHealthCheck().setRecoveryThreshold(3);
        return props;
    }

    private static ClusterConfig clusterWith(int priority) {
        ClusterConfig c = new ClusterConfig();
        c.setBootstrapServers("kafka:9092");
        c.setPriority(priority);
        return c;
    }
}
