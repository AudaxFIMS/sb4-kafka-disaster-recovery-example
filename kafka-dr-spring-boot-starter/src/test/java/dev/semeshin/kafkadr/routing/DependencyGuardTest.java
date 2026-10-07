package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterGroupConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.DependencyGate;
import dev.semeshin.kafkadr.producer.ClusterGroupUnavailableException;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.Order;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code orders-bridge} reads from core and depends on analytics; {@code scores} reads from
 * analytics and depends on nothing.
 */
class DependencyGuardTest {

    private ActiveClusterManager clusterManager;
    private BindingLifecycleManager lifecycle;
    private DependencyGuard guard;
    private MutableClock clock;

    @BeforeEach
    void setup() {
        clusterManager = mock(ActiveClusterManager.class);
        lifecycle = mock(BindingLifecycleManager.class);
        when(lifecycle.pauseConsumer(anyString(), anyString())).thenReturn(true);
        when(lifecycle.resumeConsumer(anyString(), anyString())).thenReturn(true);
        KafkaClusterProperties props = properties();
        // Pauses a gate asks for run inline here, so they can be checked right after the call.
        clock = new MutableClock(Instant.parse("2026-10-06T10:00:00Z"));
        guard = new DependencyGuard(props, props.topology(), clusterManager, lifecycle, Runnable::run, clock);
    }

    @Test
    void ownGroupStartingWhileTheDependencyIsDownStartsPaused() {
        analytics(false);

        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));

        verify(lifecycle).pauseConsumer("core-primary", "orders-bridge");
    }

    @Test
    void dependencyGoingDownPausesAndComingBackResumes() {
        analytics(true);
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));
        verify(lifecycle, never()).pauseConsumer(anyString(), anyString());

        analytics(false);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", false));
        verify(lifecycle).pauseConsumer("core-primary", "orders-bridge");

        analytics(true);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));
        verify(lifecycle).resumeConsumer("core-primary", "orders-bridge");
    }

    @Test
    void availabilityBeforeTheOwnGroupStartedIsLeftToTheSwitch() {
        analytics(false);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", false));
        // Nothing is running yet, so there is nothing to pause.
        verify(lifecycle, never()).pauseConsumer(anyString(), anyString());

        analytics(true);
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));
        verify(lifecycle, never()).pauseConsumer(anyString(), anyString());
    }

    @Test
    void consumerFailingOverWhilePausedIsPausedOnTheNewCluster() {
        analytics(false);
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));

        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-secondary"));

        verify(lifecycle).pauseConsumer("core-secondary", "orders-bridge");
        // spring-kafka keeps a pause request through stop(): the stopped core-primary binding is
        // resumed so it does not start paused, with nobody to resume it, when core fails back.
        verify(lifecycle).resumeConsumer("core-primary", "orders-bridge");
        analytics(true);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));
        verify(lifecycle).resumeConsumer("core-secondary", "orders-bridge");
    }

    @Test
    void failingBackWhilePausedLeavesNoBindingPausedBehind() {
        analytics(false);
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-secondary"));
        analytics(true);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));

        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-secondary", "core-primary"));

        // Both clusters had their pause cleared; back on core-primary nothing is paused any more.
        verify(lifecycle).resumeConsumer("core-primary", "orders-bridge");
        verify(lifecycle).resumeConsumer("core-secondary", "orders-bridge");
        verify(lifecycle, times(1)).pauseConsumer("core-primary", "orders-bridge");
    }

    @Test
    void pauseWithoutABindingToPauseIsNotRecorded() {
        // A late-initialized cluster whose binding does not exist yet.
        when(lifecycle.pauseConsumer("core-primary", "orders-bridge")).thenReturn(false, true);
        analytics(false);
        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));

        // Once the binding exists and delivers a record, the gate pauses it for real.
        assertThat(guard.gateFor("orders-bridge").blockingDependency("core-primary")).isEqualTo("analytics");

        verify(lifecycle, times(2)).pauseConsumer("core-primary", "orders-bridge");
    }

    @Test
    void switchOfAnotherGroupDoesNotTouchTheConsumer() {
        analytics(false);

        guard.onClusterSwitched(new ClusterSwitchedEvent(this, "analytics", "analytics-dc1", "analytics-dc2"));

        verify(lifecycle, never()).pauseConsumer(anyString(), anyString());
    }

    @Test
    void gateBlocksWhileTheDependencyIsDownAndPausesOnce() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(false);

        assertThat(gate.blockingDependency("core-primary")).isEqualTo("analytics");
        assertThat(gate.blockingDependency("core-primary")).isEqualTo("analytics");
        // Asked from a consumer thread, e.g. on a late-initialized cluster no switch announced.
        verify(lifecycle, times(1)).pauseConsumer("core-primary", "orders-bridge");
        assertThat(gate.nackInterval()).isEqualTo(Duration.ofMillis(500));

        analytics(true);
        assertThat(gate.blockingDependency("core-primary")).isNull();
    }

    @Test
    void gateRecognizesAnUnavailableDependencyInTheFailureChain() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        RuntimeException wrapped = new RuntimeException("handler failed", new ClusterGroupUnavailableException(
                "down", "analytics", "k1", ResilientProducer.Failure.ALL_CLUSTERS_FAILED));
        RuntimeException otherGroup = new RuntimeException(new ClusterGroupUnavailableException(
                "down", "billing", "k1", ResilientProducer.Failure.ALL_CLUSTERS_FAILED));

        assertThat(gate.isDependencyFailure("core-primary", wrapped)).isTrue();
        assertThat(gate.isDependencyFailure("core-primary", otherGroup)).isFalse();
        assertThat(gate.isDependencyFailure("core-primary", new IllegalStateException())).isFalse();
    }

    @Test
    void gateAsksForThePauseAndNeverTouchesTheBindingItself() throws Exception {
        KafkaClusterProperties props = properties();
        DependencyGuard asyncGuard = new DependencyGuard(props, clusterManager, lifecycle);
        try {
            analytics(false);
            java.util.concurrent.CountDownLatch paused = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Thread> pausingThread = new java.util.concurrent.atomic.AtomicReference<>();
            when(lifecycle.pauseConsumer("core-primary", "orders-bridge")).thenAnswer(inv -> {
                pausingThread.set(Thread.currentThread());
                paused.countDown();
                return true;
            });

            // Pausing takes the binding's monitor, which a concurrent stop holds while waiting for
            // this very consumer thread: the gate must only ask.
            assertThat(asyncGuard.gateFor("orders-bridge").blockingDependency("core-primary")).isEqualTo("analytics");

            assertThat(paused.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(pausingThread.get()).isNotSameAs(Thread.currentThread());
            assertThat(pausingThread.get().getName()).isEqualTo("kafka-dr-dependency-guard");
        } finally {
            asyncGuard.shutdown();
        }
    }

    @Test
    void gateNeverWaitsForAGuardBusyPausingABinding() throws Exception {
        KafkaClusterProperties props = properties();
        DependencyGuard asyncGuard = new DependencyGuard(props, clusterManager, lifecycle);
        try {
            analytics(true);
            asyncGuard.onClusterSwitched(new ClusterSwitchedEvent(this, "core", "core-primary", "core-primary"));
            java.util.concurrent.CountDownLatch pausing = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            when(lifecycle.pauseConsumer("core-primary", "orders-bridge")).thenAnswer(inv -> {
                pausing.countDown();
                release.await(5, java.util.concurrent.TimeUnit.SECONDS);
                return true;
            });
            analytics(false);
            Thread eventThread = new Thread(() ->
                    asyncGuard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", false)));
            eventThread.start();
            assertThat(pausing.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

            java.util.concurrent.CompletableFuture<String> answer = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> asyncGuard.gateFor("orders-bridge").blockingDependency("core-primary"));
            assertThat(answer.get(1, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("analytics");

            release.countDown();
            eventThread.join(2000);
        } finally {
            asyncGuard.shutdown();
        }
    }

    @Test
    void messageThatFailsOnItsOwnIsNotADependencyFailureEvenWithTheGroupDown() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        // Its own retries just force-marked the group down.
        analytics(false);
        RuntimeException undeliverable = new RuntimeException("handler failed",
                new dev.semeshin.kafkadr.producer.SendFailedException("not sent", "analytics", "k1",
                        ResilientProducer.Failure.SERIALIZATION));

        // Holding it back would only bring it back to fail again, forever.
        assertThat(gate.isDependencyFailure("core-primary", undeliverable)).isFalse();
        assertThat(gate.isDependencyFailure("core-primary", new IllegalStateException())).isTrue();
    }

    @Test
    void consumerWithoutDependenciesGetsNoGate() {
        assertThat(guard.gateFor("scores")).isSameAs(DependencyGate.NONE);
        assertThat(DependencyGate.NONE.blockingDependency("analytics-dc1")).isNull();
    }

    @Test
    void guardReactsToASwitchOnlyAfterTheBindingsHaveMoved() throws Exception {
        int lifecycleOrder = BindingLifecycleManager.class
                .getMethod("onClusterSwitched", ClusterSwitchedEvent.class).getAnnotation(Order.class).value();
        int guardOrder = DependencyGuard.class
                .getMethod("onClusterSwitched", ClusterSwitchedEvent.class).getAnnotation(Order.class).value();

        assertThat(lifecycleOrder).isLessThan(guardOrder);
    }

    @Test
    void aBurstOfGateChecksQueuesOnePause() {
        KafkaClusterProperties props = properties();
        List<Runnable> queued = new java.util.ArrayList<>();
        DependencyGuard queuedGuard = new DependencyGuard(props, props.topology(), clusterManager, lifecycle,
                queued::add, clock);
        DependencyGate gate = queuedGuard.gateFor("orders-bridge");
        analytics(false);

        for (int i = 0; i < 5; i++) {
            gate.blockingDependency("core-primary");
        }

        // Every held-back record asks; one request is queued until it has run.
        assertThat(queued).hasSize(1);
        queued.forEach(Runnable::run);
        verify(lifecycle, times(1)).pauseConsumer("core-primary", "orders-bridge");
    }

    @Test
    void queuedPauseIsDroppedWhenTheDependencyRecoveredFirst() {
        KafkaClusterProperties props = properties();
        List<Runnable> queued = new java.util.ArrayList<>();
        DependencyGuard queuedGuard = new DependencyGuard(props, props.topology(), clusterManager, lifecycle,
                queued::add, clock);
        DependencyGate gate = queuedGuard.gateFor("orders-bridge");
        analytics(false);
        gate.blockingDependency("core-primary");

        // The recovery — and its event, which found nothing paused — overtakes the queued pause.
        analytics(true);
        queuedGuard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));
        queued.forEach(Runnable::run);

        // Paused now, nothing would ever resume it.
        verify(lifecycle, never()).pauseConsumer(anyString(), anyString());
    }

    @Test
    void pauseAskedForByTheGateIsResumedWhenTheDependencyComesBack() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(false);
        // No switch event: a late-initialized cluster, started by a consumer thread.
        gate.blockingDependency("core-primary");
        verify(lifecycle).pauseConsumer("core-primary", "orders-bridge");

        analytics(true);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));

        verify(lifecycle).resumeConsumer("core-primary", "orders-bridge");
    }

    @Test
    void theNextOutageQueuesAPauseAgain() {
        KafkaClusterProperties props = properties();
        List<Runnable> queued = new java.util.ArrayList<>();
        DependencyGuard queuedGuard = new DependencyGuard(props, props.topology(), clusterManager, lifecycle,
                queued::add, clock);
        DependencyGate gate = queuedGuard.gateFor("orders-bridge");
        analytics(false);
        gate.blockingDependency("core-primary");
        queued.forEach(Runnable::run);
        queued.clear();
        analytics(true);
        queuedGuard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", true));

        analytics(false);
        gate.blockingDependency("core-primary");

        assertThat(queued).hasSize(1);
        queued.forEach(Runnable::run);
        verify(lifecycle, times(2)).pauseConsumer("core-primary", "orders-bridge");
    }

    @Test
    void recordIsHeldAtMostMaxHoldWhileTheDependencyIsUp() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        org.springframework.messaging.Message<?> record = record(0, 7);

        assertThat(gate.mayHoldBack("core-primary", record)).isTrue();
        clock.advance(Duration.ofSeconds(59));
        assertThat(gate.mayHoldBack("core-primary", record)).isTrue();
        clock.advance(Duration.ofSeconds(1));

        // The group is up and still does not take it: the record takes the ordinary failure path.
        assertThat(gate.mayHoldBack("core-primary", record)).isFalse();
        // And stays released through every redelivery that path makes — the binder's retries,
        // the container's error handler — instead of being held for another round.
        clock.advance(Duration.ofSeconds(1));
        assertThat(gate.mayHoldBack("core-primary", record)).isFalse();
        assertThat(guard.gateFor("orders-bridge").mayHoldBack("core-primary", record)).isFalse();
    }

    @Test
    void nextRecordOfThePartitionStartsItsOwnCount() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        gate.mayHoldBack("core-primary", record(0, 7));
        clock.advance(Duration.ofSeconds(60));
        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isFalse();

        assertThat(gate.mayHoldBack("core-primary", record(0, 8))).isTrue();
    }

    @Test
    void eachPartitionCountsItsOwnHold() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        for (int second = 0; second < 60; second += 10) {
            // Two stuck records, one per partition, alternating: neither resets the other.
            assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isTrue();
            assertThat(gate.mayHoldBack("core-primary", record(1, 3))).isTrue();
            clock.advance(Duration.ofSeconds(10));
        }
        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isFalse();
        assertThat(gate.mayHoldBack("core-primary", record(1, 3))).isFalse();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {
            org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC,
            org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION,
            org.springframework.kafka.support.KafkaHeaders.OFFSET})
    void recordThatCannotBeCountedIsNotHeld(String missingHeader) {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        org.springframework.messaging.Message<?> uncountable = org.springframework.messaging.support.MessageBuilder
                .fromMessage(record(0, 7)).removeHeader(missingHeader).build();

        // Without topic, partition and offset there is no limit to hold it within.
        assertThat(gate.mayHoldBack("core-primary", uncountable)).isFalse();

        // While the dependency is down it waits like any other record: that wait is not limited.
        analytics(false);
        assertThat(gate.mayHoldBack("core-primary", uncountable)).isTrue();
    }

    @Test
    void timeTheDependencyIsDownDoesNotCount() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        gate.mayHoldBack("core-primary", record(0, 7));
        clock.advance(Duration.ofSeconds(20));
        gate.mayHoldBack("core-primary", record(0, 7));

        // Down: the gate before the handler stops the records, the handler never runs.
        analytics(false);
        gate.blockingDependency("core-primary");
        clock.advance(Duration.ofHours(2));
        analytics(true);

        // Back: 20 s held so far, the two hours not among them.
        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isTrue();
        clock.advance(Duration.ofSeconds(30));
        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isTrue();
        // Nor does the time start over: 20 + 30 + 10 reaches the limit.
        clock.advance(Duration.ofSeconds(10));
        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isFalse();
    }

    @Test
    void anOutageAnnouncedByEventAlsoStopsTheCount() {
        DependencyGate gate = guard.gateFor("orders-bridge");
        analytics(true);
        gate.mayHoldBack("core-primary", record(0, 7));

        analytics(false);
        guard.onAvailabilityChanged(new ClusterGroupAvailabilityEvent(this, "analytics", false));
        clock.advance(Duration.ofHours(1));
        analytics(true);

        assertThat(gate.mayHoldBack("core-primary", record(0, 7))).isTrue();
    }

    private static org.springframework.messaging.Message<?> record(int partition, long offset) {
        return org.springframework.messaging.support.MessageBuilder.withPayload("order")
                .setHeader(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC, "orders")
                .setHeader(org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION, partition)
                .setHeader(org.springframework.kafka.support.KafkaHeaders.OFFSET, offset)
                .build();
    }

    private static final class MutableClock extends java.time.Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
        @Override public java.time.Clock withZone(java.time.ZoneId zone) { throw new UnsupportedOperationException(); }
        @Override public Instant instant() { return now; }
    }

    private void analytics(boolean healthy) {
        when(clusterManager.hasHealthyCluster("analytics")).thenReturn(healthy);
    }

    private static KafkaClusterProperties properties() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", group("primary", "core-a:9092", "secondary", "core-b:9092"));
        groups.put("analytics", group("dc1", "an-a:9092", "dc2", "an-b:9092"));
        props.setClusterGroups(groups);

        ConsumerConfig bridge = new ConsumerConfig();
        bridge.setTopic("orders");
        bridge.setClusterGroup("core");
        bridge.setDependsOn(List.of(" analytics "));
        bridge.setDependsOnNackIntervalMs(500);
        bridge.setDependsOnMaxHoldMs(60_000);
        ConsumerConfig scores = new ConsumerConfig();
        scores.setTopic("scores");
        scores.setClusterGroup("analytics");
        Map<String, ConsumerConfig> consumers = new LinkedHashMap<>();
        consumers.put("orders-bridge", bridge);
        consumers.put("scores", scores);
        props.setConsumers(consumers);
        return props;
    }

    private static ClusterGroupConfig group(String a, String brokersA, String b, String brokersB) {
        Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
        ClusterConfig first = new ClusterConfig();
        first.setBootstrapServers(brokersA);
        ClusterConfig second = new ClusterConfig();
        second.setBootstrapServers(brokersB);
        clusters.put(a, first);
        clusters.put(b, second);
        ClusterGroupConfig group = new ClusterGroupConfig();
        group.setClusters(clusters);
        return group;
    }
}
