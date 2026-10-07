package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import dev.semeshin.kafkadr.producer.ClusterGroupUnavailableException;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * What the consumers do with a {@link DependencyGate}: a record or batch that cannot be
 * processed because a {@code depends-on} group is down is negatively acknowledged — never
 * marked, never committed, never thrown — so it comes back instead of being skipped.
 */
class DependencyGateConsumerTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private FakeGate gate;
    private Acknowledgment ack;
    private InMemoryIdempotencyStore store;
    private LastProcessedTimestampTracker tracker;
    private List<Message<?>> handled;

    @BeforeEach
    void setup() {
        gate = new FakeGate();
        ack = mock(Acknowledgment.class);
        store = new InMemoryIdempotencyStore();
        tracker = new LastProcessedTimestampTracker(null);
        handled = new ArrayList<>();
    }

    // --- record path ------------------------------------------------------------------

    @Test
    void blockedRecordIsHeldBackWithoutReachingTheHandlerOrTheStore() {
        gate.blocking = "analytics";
        Message<?> message = record(0);

        recordConsumer(handled::add).accept(message);

        assertThat(handled).isEmpty();
        verify(ack).nack(INTERVAL);
        verify(ack, never()).acknowledge();
        // Not marked: when it comes back it is processed, not dropped as a duplicate.
        gate.blocking = null;
        recordConsumer(handled::add).accept(message);
        assertThat(handled).hasSize(1);
    }

    @Test
    void handlerFailureCausedByTheDependencyIsHeldBackInsteadOfThrown() {
        gate.dependencyFailure = true;
        Message<?> message = record(0);

        recordConsumer(m -> {
            throw new ClusterGroupUnavailableException("down", "analytics", "k0",
                    ResilientProducer.Failure.NO_HEALTHY_CLUSTER);
        }).accept(message);

        verify(ack).nack(INTERVAL);
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
        // The mark taken before the handler ran was released.
        assertThat(store.tryProcess("primary", "orders", message)).isTrue();
    }

    @Test
    void recordHeldBackPastMaxHoldTakesTheOrdinaryFailurePath() {
        gate.dependencyFailure = true;
        gate.maxHoldReached = true;
        Message<?> message = record(0);

        assertThatThrownBy(() -> recordConsumer(m -> {
            throw new ClusterGroupUnavailableException("not taken", "analytics", "k0",
                    ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        }).accept(message)).isInstanceOf(ClusterGroupUnavailableException.class);

        verify(ack, never()).nack(any(Duration.class));
        // Released all the same, so the binder's retries are not dropped as duplicates.
        assertThat(store.tryProcess("primary", "orders", message)).isTrue();
    }

    @Test
    void otherHandlerFailuresStillPropagate() {
        Consumer<Message<?>> failing = m -> {
            throw new IllegalArgumentException("bad payload");
        };

        assertThatThrownBy(() -> recordConsumer(failing).accept(record(0)))
                .isInstanceOf(IllegalArgumentException.class);
        verify(ack, never()).nack(any(Duration.class));
    }

    @Test
    void blockedRecordWithoutAnAcknowledgmentCannotBeHeldBackAndFails() {
        gate.blocking = "analytics";
        Message<?> withoutAck = MessageBuilder.withPayload("p").setHeader(KafkaHeaders.RECEIVED_KEY, "k").build();

        assertThatThrownBy(() -> recordConsumer(handled::add).accept(withoutAck))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dependency group 'analytics' unavailable");
        assertThat(handled).isEmpty();
    }

    // --- split batch -------------------------------------------------------------------

    @Test
    void blockedBatchIsHeldBackFromItsFirstRecord() {
        gate.blocking = "analytics";
        List<List<Message<?>>> batches = new ArrayList<>();

        splitConsumer(messages -> {
            batches.add(messages);
            return BatchHandler.Result.COMPLETE;
        }).accept(batchOf(3));

        assertThat(batches).isEmpty();
        verify(ack).nack(0, INTERVAL);
        verify(ack, never()).acknowledge();
        assertThat(store.tryProcess("primary", "orders", record(1))).isTrue();
    }

    @Test
    void batchStoppedByTheDependencyCommitsThePrefixAndHoldsTheRestBack() {
        gate.dependencyFailure = true;

        splitConsumer(messages -> BatchHandler.Result.stoppedAt(2, messages,
                new ClusterGroupUnavailableException("down", "analytics", null,
                        ResilientProducer.Failure.ALL_CLUSTERS_FAILED)))
                .accept(batchOf(4));

        // nack(2) commits records 0 and 1 and brings 2 and 3 back after the interval.
        verify(ack).nack(2, INTERVAL);
        verify(ack, never()).acknowledge(anyInt());
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1001L);
        // The tail's marks were released, the prefix keeps its own.
        assertThat(store.tryProcess("primary", "orders", record(2))).isTrue();
        assertThat(store.tryProcess("primary", "orders", record(1))).isFalse();
    }

    @Test
    void batchRecordHeldBackPastMaxHoldFailsTheBatchFromThatRecord() {
        gate.dependencyFailure = true;
        gate.maxHoldReached = true;
        Message<?> batch = batchOf(4);

        assertThatThrownBy(() -> splitConsumer(messages -> BatchHandler.Result.stoppedAt(2, messages,
                new ClusterGroupUnavailableException("not taken", "analytics", null,
                        ResilientProducer.Failure.ALL_CLUSTERS_FAILED)))
                .accept(batch))
                .isInstanceOf(org.springframework.kafka.listener.BatchListenerFailedException.class);

        // The limit is checked for the record that stopped the batch.
        assertThat(gate.askedAbout.getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(2L);
        verify(ack, never()).nack(anyInt(), any(Duration.class));
    }

    @Test
    void batchStoppedByAnythingElseStillFailsAsBefore() {
        assertThatThrownBy(() -> splitConsumer(messages -> BatchHandler.Result.stoppedAt(1, messages,
                new IllegalStateException("boom"))).accept(batchOf(3)))
                .isInstanceOf(org.springframework.kafka.listener.BatchListenerFailedException.class);
        verify(ack, never()).nack(anyInt(), any(Duration.class));
    }

    // --- standard batch ------------------------------------------------------------------

    @Test
    void blockedStandardBatchNeverReachesTheHandler() {
        gate.blocking = "analytics";

        new BatchPassThroughConsumer("orders", "primary", handled::add, tracker,
                new AckPolicy(AckMode.MANUAL, AckPolicy.Owner.HANDLER, false), gate)
                .accept(batchOf(3));

        assertThat(handled).isEmpty();
        verify(ack).nack(0, INTERVAL);
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    // --- fixtures -------------------------------------------------------------------------

    private IdempotentConsumer recordConsumer(Consumer<Message<?>> handler) {
        return new IdempotentConsumer("orders", "primary", store, handler, tracker,
                new AckPolicy(AckMode.MANUAL, AckPolicy.Owner.STARTER, false), gate);
    }

    private BatchIdempotentConsumer splitConsumer(BatchHandler handler) {
        return new BatchIdempotentConsumer("orders", "primary", store, handler, tracker,
                AckMode.MANUAL_IMMEDIATE, gate);
    }

    private Message<?> record(int index) {
        return MessageBuilder.withPayload("p" + index)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "k" + index)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "orders")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1000L + index)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();
    }

    private Message<?> batchOf(int size) {
        return MessageBuilder.fromMessage(BatchMessagesTest.envelope(
                        IntStream.range(0, size).mapToObj(i -> "p" + i).toList(),
                        IntStream.range(0, size).mapToObj(i -> "k" + i).toList(),
                        IntStream.range(0, size).mapToObj(i -> "orders").toList(),
                        IntStream.range(0, size).mapToObj(i -> 0).toList(),
                        IntStream.range(0, size).mapToObj(i -> (long) i).toList(),
                        IntStream.range(0, size).mapToObj(i -> 1000L + i).toList(),
                        IntStream.range(0, size).mapToObj(i -> Map.of()).toList()))
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();
    }

    /** A gate the test opens and closes directly. */
    private static final class FakeGate implements DependencyGate {
        String blocking;
        boolean dependencyFailure;
        boolean maxHoldReached;
        Message<?> askedAbout;

        @Override
        public String blockingDependency(String clusterName) {
            return blocking;
        }

        @Override
        public boolean isDependencyFailure(String clusterName, Throwable failure) {
            return dependencyFailure || blocking != null;
        }

        @Override
        public Duration nackInterval() {
            return INTERVAL;
        }

        @Override
        public boolean mayHoldBack(String clusterName, Message<?> record) {
            askedAbout = record;
            return !maxHoldReached;
        }
    }
}
