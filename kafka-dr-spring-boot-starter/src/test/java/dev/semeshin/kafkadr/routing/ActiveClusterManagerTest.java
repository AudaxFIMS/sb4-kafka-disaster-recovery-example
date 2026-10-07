package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.routing.FailoverStateStore.FailoverState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

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
        assertThatThrownBy(() -> manager(props, publisher, new InMemoryFailoverStateStore()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void initialActiveIsFirstByPriority() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(mgr.getClustersByPriority()).containsExactly("primary", "secondary", "tertiary");
    }

    @Test
    void hasHealthyClusterIsFalseUntilAClusterReportsHealthy() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

        // All clusters start unhealthy (e.g. nothing reachable at startup)
        assertThat(mgr.hasHealthyCluster()).isFalse();

        mgr.reportHealth("secondary", true);
        assertThat(mgr.hasHealthyCluster()).isTrue();
    }

    @Test
    void firstHealthyClusterIsElectedImmediatelyOnInitialReport() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(mgr.getHealthStatuses()).containsEntry("primary", true);
    }

    @Test
    void failoverWhenActiveClusterCrossesFailureThreshold() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

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
        ActiveClusterManager mgr = manager(props, publisher, store);

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
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

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
        ActiveClusterManager mgr = manager(props, publisher, store);

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

        ActiveClusterManager mgr = manager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void clearsStoreWhenPersistedFailoverIsPastThreshold() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("12:00:00");

        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now().minusSeconds(86400 * 2)));

        ActiveClusterManager mgr = manager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void failbackThresholdIsBumpedToNextDayWhenFailoverWasAfterFailbackTime() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("02:00:00");
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        // Failover at 23:00: the window opens at 02:00 the next day, not at 02:00 the same day.
        store.save(new FailoverState("secondary", Instant.parse("2026-10-05T23:00:00Z")));

        TestClock night = new TestClock("2026-10-06T01:30:00Z");
        assertThat(manager(props, publisher, store, night).getActiveCluster()).isEqualTo("secondary");

        TestClock morning = new TestClock("2026-10-06T02:30:00Z");
        assertThat(manager(props, publisher, store, morning).getActiveCluster()).isEqualTo("primary");
    }

    @Test
    void isFailbackBlockedReturnsFalseWhenFailoverAtIsCleared() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("23:59:59");

        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

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

        ActiveClusterManager mgr = manager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void clearsStoreWhenFailbackAfterNotConfigured() {
        KafkaClusterProperties props = threeClusters();
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now()));

        ActiveClusterManager mgr = manager(props, publisher, store);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void forceUnhealthyOnAlreadyUnhealthyClusterIsNoOp() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("secondary", true);
        events.clear();

        mgr.forceUnhealthy("primary");

        assertThat(events).isEmpty();
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void allClustersUnhealthyDoesNotMutateActive() {
        KafkaClusterProperties props = threeClusters();
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());

        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", false);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
    }

    @Test
    void explicitGroupTakesThresholdsAndFailbackFromItsOverrides() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.getHealthCheck().setFailureThreshold(3);
        props.getFailover().setFailbackAfter("23:59:59");
        KafkaClusterProperties.ClusterGroupConfig core = new KafkaClusterProperties.ClusterGroupConfig();
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put("secondary", clusterWith(2));
        clusters.put("primary", clusterWith(1));
        core.setClusters(clusters);
        core.getHealthCheck().setFailureThreshold(1);
        core.getFailover().setFailbackAfter("");
        props.setClusterGroups(Map.of("core", core));

        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());
        assertThat(mgr.getClustersByPriority()).containsExactly("core-primary", "core-secondary");
        assertThat(mgr.getActiveCluster()).isEqualTo("core-primary");

        mgr.reportHealth("core-primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("core-secondary", true);

        // failure-threshold 1 from the group, not 3 from the global settings.
        mgr.reportHealth("core-primary", false);
        assertThat(mgr.getActiveCluster()).isEqualTo("core-secondary");

        // The group switched the global failback window off, so failback is immediate.
        for (int i = 0; i < 3; i++) mgr.reportHealth("core-primary", true);
        assertThat(mgr.getActiveCluster()).isEqualTo("core-primary");
    }

    // --- several cluster groups -----------------------------------------------------

    @Test
    void groupsFailOverIndependently() {
        ActiveClusterManager mgr = manager(twoGroups(), publisher, new InMemoryFailoverStateStore());
        allHealthy(mgr);
        events.clear();

        mgr.forceUnhealthy("core-primary");

        assertThat(mgr.getActiveCluster("core")).isEqualTo("core-secondary");
        assertThat(mgr.getActiveCluster("analytics")).isEqualTo("analytics-dc1");
        assertThat(mgr.getActiveClusters()).containsExactly(
                Map.entry("core", "core-secondary"), Map.entry("analytics", "analytics-dc1"));
        assertThat(events).singleElement().isInstanceOfSatisfying(ClusterSwitchedEvent.class, e -> {
            assertThat(e.getGroup()).isEqualTo("core");
            assertThat(e.getPreviousCluster()).isEqualTo("core-primary");
            assertThat(e.getNewCluster()).isEqualTo("core-secondary");
        });
    }

    @Test
    void eachGroupUsesItsOwnThresholds() {
        KafkaClusterProperties props = twoGroups();
        props.getClusterGroups().get("analytics").getHealthCheck().setFailureThreshold(1);
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore());
        allHealthy(mgr);

        mgr.reportHealth("core-primary", false);
        mgr.reportHealth("analytics-dc1", false);

        // Global threshold 3 for core, group threshold 1 for analytics.
        assertThat(mgr.getActiveCluster("core")).isEqualTo("core-primary");
        assertThat(mgr.getActiveCluster("analytics")).isEqualTo("analytics-dc2");
    }

    @Test
    void groupWithoutHealthyClustersIsReportedOnTransitionsOnly() {
        ActiveClusterManager mgr = manager(twoGroups(), publisher, new InMemoryFailoverStateStore());
        assertThat(mgr.hasHealthyCluster("analytics")).isFalse();

        mgr.reportHealth("analytics-dc1", true);
        mgr.reportHealth("analytics-dc2", true);
        assertThat(availability()).containsExactly("analytics=true");

        mgr.forceUnhealthy("analytics-dc1");
        mgr.forceUnhealthy("analytics-dc2");
        // Published by the producer's own report, not left for the next health round: depends-on
        // pauses its consumers on this event.
        assertThat(availability()).containsExactly("analytics=true", "analytics=false");
        mgr.reportHealth("analytics-dc2", false);
        assertThat(mgr.hasHealthyCluster("analytics")).isFalse();
        assertThat(availability()).containsExactly("analytics=true", "analytics=false");
        // The other group is not affected and was never reported.
        assertThat(mgr.hasHealthyCluster("core")).isFalse();
    }

    @Test
    void failoverStateIsPersistedAndRestoredPerGroup() {
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        KafkaClusterProperties props = twoGroups();
        props.getFailover().setFailbackAfter(LocalTime.now().plusHours(2).withNano(0).toString());

        ActiveClusterManager first = manager(props, publisher, store);
        allHealthy(first);
        first.forceUnhealthy("analytics-dc1");
        // Each group under its own key: analytics has its failover, core — on its preferred
        // cluster since startup — nothing to hold.
        assertThat(store.load("analytics")).get().extracting(FailoverState::activeCluster).isEqualTo("analytics-dc2");
        assertThat(store.load("core")).isEmpty();

        ActiveClusterManager restarted = manager(props, publisher, store);
        assertThat(restarted.getActiveCluster("analytics")).isEqualTo("analytics-dc2");
        assertThat(restarted.getActiveCluster("core")).isEqualTo("core-primary");
    }

    @Test
    void storeWithoutPerGroupMethodsIsRefusedForSeveralGroups() {
        FailoverStateStore singleState = new FailoverStateStore() {
            @Override public void save(FailoverState state) { }
            @Override public java.util.Optional<FailoverState> load() { return java.util.Optional.empty(); }
            @Override public void clear() { }
        };

        assertThatThrownBy(() -> manager(twoGroups(), publisher, singleState))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not keep failover state per cluster group");
        // One group is fine: the per-group defaults delegate to the single state.
        assertThat(manager(threeClusters(), publisher, singleState).getActiveCluster())
                .isEqualTo("primary");
    }

    @Test
    void singleGroupAccessorsRefuseSeveralGroups() {
        ActiveClusterManager mgr = manager(twoGroups(), publisher, new InMemoryFailoverStateStore());

        assertThatThrownBy(mgr::getActiveCluster)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("getActiveCluster(group)");
        assertThatThrownBy(mgr::hasHealthyCluster).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(mgr::getClustersByPriority).isInstanceOf(IllegalStateException.class);
        assertThat(mgr.getGroups()).containsExactly("core", "analytics");
        assertThat(mgr.getGroupOfCluster("analytics-dc2")).isEqualTo("analytics");
        assertThat(mgr.getClustersByPriority("analytics")).containsExactly("analytics-dc1", "analytics-dc2");
        assertThat(mgr.getHealthStatuses()).containsOnlyKeys(
                "core-primary", "core-secondary", "analytics-dc1", "analytics-dc2");
    }

    @Test
    void unknownClusterOrGroupIsRejected() {
        ActiveClusterManager mgr = manager(twoGroups(), publisher, new InMemoryFailoverStateStore());

        assertThatThrownBy(() -> mgr.reportHealth("primary", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown cluster 'primary'");
        assertThatThrownBy(() -> mgr.getActiveCluster("billing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown cluster group 'billing'");
    }

    @Test
    void clusterRestoredFromTheStoreIsAnnouncedOnceItIsHealthy() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter(LocalTime.now().plusHours(2).withNano(0).toString());
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        FailoverState persisted = new FailoverState("secondary", Instant.now().minusSeconds(60));
        store.save(persisted);
        ActiveClusterManager mgr = manager(props, publisher, store);

        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);

        // Nothing had started its bindings: without this switch its consumers never consume.
        assertThat(events).filteredOn(ClusterSwitchedEvent.class::isInstance).singleElement()
                .isInstanceOfSatisfying(ClusterSwitchedEvent.class, e -> {
                    assertThat(e.getPreviousCluster()).isEqualTo("secondary");
                    assertThat(e.getNewCluster()).isEqualTo("secondary");
                });
        // The persisted failover is untouched: the failback window still applies.
        assertThat(store.load()).contains(persisted);

        events.clear();
        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);
        assertThat(events).noneMatch(ClusterSwitchedEvent.class::isInstance);
    }

    @Test
    void producerReportingAFailureNeverWaitsForAListenerOfTheSameGroup() throws Exception {
        java.util.concurrent.CountDownLatch inListener = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<ApplicationEvent> published = java.util.Collections.synchronizedList(new ArrayList<>());
        ApplicationEventPublisher slowPublisher = event -> {
            published.add((ApplicationEvent) event);
            if (event instanceof ClusterSwitchedEvent switched && switched.getNewCluster().equals("secondary")) {
                // A switch stopping containers: blocks until the listener thread finishes.
                inListener.countDown();
                try {
                    release.await(5, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        ActiveClusterManager mgr = manager(threeClusters(), slowPublisher, new InMemoryFailoverStateStore());
        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) {
            mgr.reportHealth("secondary", true);
            mgr.reportHealth("tertiary", true);
        }

        Thread health = new Thread(() -> mgr.forceUnhealthy("primary"));
        health.start();
        assertThat(inListener.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // Meanwhile a send on another thread finds secondary dead as well.
        Thread producer = new Thread(() -> mgr.forceUnhealthy("secondary"));
        producer.start();
        producer.join(2000);
        assertThat(producer.isAlive()).as("producer thread blocked behind the publishing listener").isFalse();
        assertThat(mgr.getActiveCluster()).isEqualTo("tertiary");

        release.countDown();
        health.join(2000);
        // Its switch was queued and published by the thread already publishing, in order.
        assertThat(published).filteredOn(ClusterSwitchedEvent.class::isInstance)
                .extracting(e -> ((ClusterSwitchedEvent) e).getNewCluster())
                .endsWith("secondary", "tertiary");
    }

    @Test
    void proxiedStoreAnswersForItsTarget() {
        FailoverStateStore singleState = new FailoverStateStore() {
            @Override public void save(FailoverState state) { }
            @Override public java.util.Optional<FailoverState> load() { return java.util.Optional.empty(); }
            @Override public void clear() { }
        };
        org.springframework.aop.framework.ProxyFactory factory = new org.springframework.aop.framework.ProxyFactory(singleState);
        factory.addInterface(FailoverStateStore.class);
        FailoverStateStore proxied = (FailoverStateStore) factory.getProxy();

        // supportsGroups() is declared, not inferred from the class's methods: a proxy, which
        // declares every interface method itself, simply forwards what its target says.
        assertThatThrownBy(() -> manager(twoGroups(), publisher, proxied))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not keep failover state per cluster group");
        assertThat(manager(twoGroups(), publisher,
                (FailoverStateStore) new org.springframework.aop.framework.ProxyFactory(new InMemoryFailoverStateStore())
                        .getProxy())
                .getGroups()).containsExactly("core", "analytics");
    }

    @Test
    void restoredClusterWinsTheInitialElectionEvenWhenThePrimaryReportsFirst() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter(LocalTime.now().plusHours(2).withNano(0).toString());
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        FailoverState persisted = new FailoverState("secondary", Instant.now().minusSeconds(60));
        store.save(persisted);
        ActiveClusterManager mgr = manager(props, publisher, store);

        // One health round: clusters report in priority order, primary first.
        mgr.reportHealth("primary", true);
        assertThat(events).noneMatch(ClusterSwitchedEvent.class::isInstance);
        mgr.reportHealth("secondary", true);

        // Elected at once, without waiting out recovery-threshold, and the window still holds.
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(store.load()).contains(persisted);
        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", true);
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void failbackWindowNeverKeepsTheGroupOnAClusterThatIsDown() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("23:00:00");
        TestClock clock = new TestClock("2026-10-06T10:00:00Z");
        ActiveClusterManager mgr = manager(props, publisher, new InMemoryFailoverStateStore(), clock);
        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("tertiary", true);
        mgr.forceUnhealthy("primary");
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        for (int i = 0; i < 3; i++) mgr.reportHealth("primary", true);
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");

        // Inside the window, yet the cluster it holds the group on is gone: leaving it is a
        // failover, and the best healthy cluster is the primary.
        mgr.forceUnhealthy("secondary");

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
    }

    @Test
    void electingThePreferredClusterAtStartupIsNoFailover() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("23:59:59");
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        ActiveClusterManager mgr = manager(props, publisher, store);

        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        // Nothing to hold for failback-after, so nothing persisted to restore on the next start.
        assertThat(store.load()).isEmpty();
    }

    @Test
    void failbackHappensOnceTheWindowOpensWithoutAnotherHealthTransition() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter("12:00:00");
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.parse("2026-10-06T09:00:00Z")));
        TestClock clock = new TestClock("2026-10-06T10:00:00Z");
        ActiveClusterManager mgr = manager(props, publisher, store, clock);
        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", true);
        mgr.reportHealth("primary", true);
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");

        clock.set("2026-10-06T12:00:01Z");
        // The primary has been healthy all along: only its next report can notice the window.
        mgr.reportHealth("primary", true);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(store.load()).isEmpty();
    }

    @Test
    void groupIsNotServingWhileTheRestoredClusterHasNotReported() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter(LocalTime.now().plusHours(2).withNano(0).toString());
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now().minusSeconds(60)));
        ActiveClusterManager mgr = manager(props, publisher, store);

        mgr.reportHealth("primary", true);

        // Active is still the restored cluster nobody has checked: a send must not go there yet.
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        assertThat(mgr.hasHealthyCluster("default")).isFalse();
        assertThat(availability()).isEmpty();

        mgr.reportHealth("secondary", true);
        assertThat(mgr.hasHealthyCluster("default")).isTrue();
        assertThat(availability()).containsExactly("default=true");
    }

    @Test
    void restoredClusterThatIsDownLosesTheInitialElection() {
        KafkaClusterProperties props = threeClusters();
        props.getFailover().setFailbackAfter(LocalTime.now().plusHours(2).withNano(0).toString());
        InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();
        store.save(new FailoverState("secondary", Instant.now().minusSeconds(60)));
        ActiveClusterManager mgr = manager(props, publisher, store);

        mgr.reportHealth("primary", true);
        mgr.reportHealth("secondary", false);

        assertThat(mgr.getActiveCluster()).isEqualTo("primary");
        assertThat(events).filteredOn(ClusterSwitchedEvent.class::isInstance).singleElement()
                .isInstanceOfSatisfying(ClusterSwitchedEvent.class, e -> assertThat(e.getNewCluster()).isEqualTo("primary"));
        // Leaving the restored cluster for a higher-priority one is a failback: the state goes.
        assertThat(store.load()).isEmpty();
    }

    @Test
    void producerForcedFailoverIsPublishedOffTheCallersThread() throws Exception {
        java.util.concurrent.CountDownLatch switched = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Thread> publishingThread = new java.util.concurrent.atomic.AtomicReference<>();
        ApplicationEventPublisher recording = event -> {
            if (event instanceof ClusterSwitchedEvent e && e.getNewCluster().equals("secondary")) {
                publishingThread.set(Thread.currentThread());
                switched.countDown();
            }
        };
        // The production wiring: the manager's own publisher thread.
        ActiveClusterManager mgr = new ActiveClusterManager(threeClusters(), recording, new InMemoryFailoverStateStore());
        try {
            mgr.reportHealth("primary", true);
            for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);

            mgr.forceUnhealthy("primary");

            // The state moved at once: the producer's next send already goes to secondary.
            assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
            assertThat(switched.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(publishingThread.get()).isNotSameAs(Thread.currentThread());
            assertThat(publishingThread.get().getName()).isEqualTo("kafka-dr-failover-events-default");
        } finally {
            mgr.shutdown();
        }
    }

    @Test
    void eachGroupPublishesItsForcedFailoversOnItsOwnThread() throws Exception {
        java.util.concurrent.CountDownLatch coreStopping = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch analyticsSwitched = new java.util.concurrent.CountDownLatch(1);
        ApplicationEventPublisher slow = event -> {
            if (event instanceof ClusterSwitchedEvent e && e.getNewCluster().equals("core-secondary")) {
                coreStopping.countDown();
                try {
                    release.await(5, java.util.concurrent.TimeUnit.SECONDS);   // core's containers stopping
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            if (event instanceof ClusterSwitchedEvent e && e.getNewCluster().equals("analytics-dc2")) {
                analyticsSwitched.countDown();
            }
        };
        ActiveClusterManager mgr = new ActiveClusterManager(twoGroups(), slow, new InMemoryFailoverStateStore());
        try {
            for (String cluster : List.of("core-primary", "core-secondary", "analytics-dc1", "analytics-dc2")) {
                for (int i = 0; i < 3; i++) mgr.reportHealth(cluster, true);
            }

            mgr.forceUnhealthy("core-primary");
            assertThat(coreStopping.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            mgr.forceUnhealthy("analytics-dc1");

            // Analytics' switch does not queue behind core's container stop.
            assertThat(analyticsSwitched.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            release.countDown();
            mgr.shutdown();
        }
    }

    @Test
    void forcedFailoverAfterShutdownStillEndsNormally() {
        ActiveClusterManager mgr = new ActiveClusterManager(threeClusters(), publisher, new InMemoryFailoverStateStore());
        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);
        mgr.shutdown();

        // A send failing during shutdown must end in a SendResult, not RejectedExecutionException.
        assertThatCode(() -> mgr.forceUnhealthy("primary")).doesNotThrowAnyException();
        assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
    }

    @Test
    void slowFailoverStateStoreNeverBlocksTheSenderNorTheGroup() throws Exception {
        java.util.concurrent.CountDownLatch saving = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        InMemoryFailoverStateStore slowStore = new InMemoryFailoverStateStore() {
            @Override
            public void save(String group, FailoverState state) {
                if (state.activeCluster().equals("secondary")) {
                    saving.countDown();
                    try {
                        release.await(5, java.util.concurrent.TimeUnit.SECONDS);   // Redis is slow
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.save(group, state);
            }
        };
        ActiveClusterManager mgr = new ActiveClusterManager(threeClusters(), publisher, slowStore);
        try {
            mgr.reportHealth("primary", true);
            for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);

            java.util.concurrent.CompletableFuture<Void> send =
                    java.util.concurrent.CompletableFuture.runAsync(() -> mgr.forceUnhealthy("primary"));
            send.get(1, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(saving.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

            // While the store is still busy, the group keeps taking reports.
            java.util.concurrent.CompletableFuture.runAsync(() -> mgr.reportHealth("tertiary", true))
                    .get(1, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(mgr.getActiveCluster()).isEqualTo("secondary");
        } finally {
            release.countDown();
            mgr.shutdown();
        }
    }

    @Test
    void shutdownLetsAQueuedFailoverStateSaveFinish() {
        InMemoryFailoverStateStore slowStore = new InMemoryFailoverStateStore() {
            @Override
            public void save(String group, FailoverState state) {
                try {
                    Thread.sleep(200);   // Redis round trip
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                super.save(group, state);
            }
        };
        ActiveClusterManager mgr = new ActiveClusterManager(threeClusters(), publisher, slowStore);
        mgr.reportHealth("primary", true);
        for (int i = 0; i < 3; i++) mgr.reportHealth("secondary", true);

        mgr.forceUnhealthy("primary");
        mgr.shutdown();

        // Dropping it would make the next start elect the cluster that just failed.
        assertThat(slowStore.load()).get().extracting(FailoverState::activeCluster).isEqualTo("secondary");
    }

    /**
     * A manager whose producer-forced failovers are published on the calling thread, so tests can
     * check events right after the call.
     */
    private static ActiveClusterManager manager(KafkaClusterProperties props, ApplicationEventPublisher publisher,
                                                FailoverStateStore store) {
        return manager(props, publisher, store, Clock.systemDefaultZone());
    }

    private static ActiveClusterManager manager(KafkaClusterProperties props, ApplicationEventPublisher publisher,
                                                FailoverStateStore store, Clock clock) {
        return new ActiveClusterManager(props.topology(), publisher, store, Runnable::run, clock);
    }

    /** A clock the test moves forward; failback-after is read against it. */
    private static final class TestClock extends Clock {
        private Instant now;
        private final ZoneId zone = ZoneId.of("UTC");

        TestClock(String isoInstant) {
            this.now = Instant.parse(isoInstant);
        }

        void set(String isoInstant) {
            this.now = Instant.parse(isoInstant);
        }

        @Override public ZoneId getZone() { return zone; }
        @Override public Clock withZone(ZoneId zone) { throw new UnsupportedOperationException(); }
        @Override public Instant instant() { return now; }
    }

    private List<String> availability() {
        return events.stream()
                .filter(ClusterGroupAvailabilityEvent.class::isInstance)
                .map(ClusterGroupAvailabilityEvent.class::cast)
                .map(e -> e.getGroup() + "=" + e.isAvailable())
                .toList();
    }

    private static void allHealthy(ActiveClusterManager mgr) {
        for (String cluster : List.of("core-primary", "core-secondary", "analytics-dc1", "analytics-dc2")) {
            for (int i = 0; i < 3; i++) mgr.reportHealth(cluster, true);
        }
    }

    private static KafkaClusterProperties twoGroups() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.getHealthCheck().setFailureThreshold(3);
        props.getHealthCheck().setRecoveryThreshold(3);
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", group("primary", 1, "secondary", 2));
        groups.put("analytics", group("dc1", 1, "dc2", 2));
        props.setClusterGroups(groups);
        return props;
    }

    private static KafkaClusterProperties.ClusterGroupConfig group(String a, int pa, String b, int pb) {
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        clusters.put(a, clusterWith(pa));
        clusters.put(b, clusterWith(pb));
        group.setClusters(clusters);
        return group;
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
