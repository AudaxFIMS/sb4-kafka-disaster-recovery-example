package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Acknowledgment on the record path: who commits, and when the timestamp watermark is
 * allowed to follow. The watermark must never move ahead of the committed offset — after a
 * failover the seek would start past records that nothing redelivers.
 */
class RecordAcknowledgmentTest {

    private Acknowledgment ack;
    private LastProcessedTimestampTracker tracker;

    @BeforeEach
    void setUp() {
        ack = mock(Acknowledgment.class);
        tracker = new LastProcessedTimestampTracker(null);
    }

    @Test
    void starterAcknowledgesAfterTheHandlerAndAdvancesTheWatermark() {
        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.STARTER), msg -> { }).accept(record());

        verify(ack).acknowledge();
        assertThat(tracker.getLastTimestamp("order-events", 2)).isEqualTo(1714003200000L);
    }

    @Test
    void starterDoesNotAcknowledgeWhenTheHandlerFails() {
        Consumer<Message<?>> failing = msg -> { throw new IllegalStateException("boom"); };

        assertThatThrownBy(() -> consumer(policy(AckMode.MANUAL_IMMEDIATE, AckPolicy.Owner.STARTER), failing)
                .accept(record()))
                .isInstanceOf(IllegalStateException.class);

        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void handlerOwnedAcknowledgmentReachesTheContainerAndAdvancesTheWatermark() {
        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.HANDLER), RecordAcknowledgmentTest::acknowledge)
                .accept(record());

        verify(ack).acknowledge();
        assertThat(tracker.getLastTimestamp("order-events", 2)).isEqualTo(1714003200000L);
    }

    @Test
    void handlerThatNeverAcknowledgesLeavesTheWatermarkAlone() {
        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.HANDLER), msg -> { }).accept(record());

        // Nothing was committed, so a watermark advanced here would make seek-by-timestamp
        // skip this record after a failover.
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void nackIsPassedThroughAndDoesNotAdvanceTheWatermark() {
        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.HANDLER),
                msg -> acknowledgment(msg).nack(Duration.ofSeconds(1))).accept(record());

        verify(ack).nack(Duration.ofSeconds(1));
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void asyncAcksAllowTheHandlerToAcknowledgeLater() {
        AckPolicy async = new AckPolicy(AckMode.MANUAL, AckPolicy.Owner.HANDLER, true);

        consumer(async, msg -> { }).accept(record());

        // The commit has not happened yet, so the watermark stays put — but this is the
        // documented way to work, not a forgotten acknowledgment.
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void containerManagedModesStillAdvanceTheWatermark() {
        for (AckMode mode : new AckMode[] {null, AckMode.BATCH, AckMode.RECORD}) {
            tracker = new LastProcessedTimestampTracker(null);

            consumer(policy(mode, AckPolicy.Owner.HANDLER), msg -> { }).accept(record());

            assertThat(tracker.getLastTimestamp("order-events", 2))
                    .as("ack-mode=%s", mode)
                    .isEqualTo(1714003200000L);
        }
    }

    @Test
    void scheduledModesDoNotAdvanceTheWatermark() {
        for (AckMode mode : List.of(AckMode.TIME, AckMode.COUNT, AckMode.COUNT_TIME)) {
            tracker = new LastProcessedTimestampTracker(null);

            consumer(policy(mode, AckPolicy.Owner.HANDLER), msg -> { }).accept(record());

            // These commit on the container's own schedule, which the starter cannot observe.
            assertThat(tracker.getAllTimestamps()).as("ack-mode=%s", mode).isEmpty();
        }
    }

    @Test
    void manualModeWithoutTheHeaderIsSurvivable() {
        Message<?> withoutAck = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "order-events")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 2)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build();

        assertThatCode(() -> consumer(policy(AckMode.MANUAL, AckPolicy.Owner.STARTER), msg -> { })
                .accept(withoutAck)).doesNotThrowAnyException();

        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void theHandlerKeepsEveryOtherHeaderWhenTheAcknowledgmentIsWrapped() {
        Message<?>[] seen = new Message<?>[1];

        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.HANDLER), msg -> seen[0] = msg).accept(record());

        assertThat(seen[0].getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("order-events");
        assertThat(seen[0].getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("o-1");
        assertThat(seen[0].getPayload()).isEqualTo("payload");
        assertThat(seen[0].getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isNotSameAs(ack);
    }

    @Test
    void starterOwnedDeliveryLeavesTheMessageUntouched() {
        Message<?> original = record();
        Message<?>[] seen = new Message<?>[1];

        consumer(policy(AckMode.MANUAL, AckPolicy.Owner.STARTER), msg -> seen[0] = msg).accept(original);

        // Nothing to wrap: the handler is not the one acknowledging.
        assertThat(seen[0]).isSameAs(original);
    }

    @Test
    void duplicatesAreAcknowledgedUnderEveryManualOwner() {
        for (AckPolicy.Owner owner : AckPolicy.Owner.values()) {
            ack = mock(Acknowledgment.class);
            tracker = new LastProcessedTimestampTracker(null);

            duplicateConsumer(policy(AckMode.MANUAL, owner)).accept(record());

            // The handler never sees a duplicate, so nobody else could commit it. Leaving it
            // unacknowledged stalls the offset on an all-duplicate stretch — the normal state
            // right after a failover — until the idempotency marks expire and the redelivery
            // is processed for real.
            verify(ack).acknowledge();
            // The record was processed by an earlier delivery, which already moved the watermark.
            assertThat(tracker.getAllTimestamps()).as("owner=%s", owner).isEmpty();
        }
    }

    @Test
    void duplicatesTouchNothingUnderContainerManagedModes() {
        duplicateConsumer(policy(AckMode.BATCH, AckPolicy.Owner.HANDLER)).accept(record());

        // The container commits on its own here; acknowledging would be meaningless.
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    private IdempotentConsumer duplicateConsumer(AckPolicy policy) {
        IdempotencyStore store = mock(IdempotencyStore.class);
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(false);
        return new IdempotentConsumer("orders", "primary", store, msg -> { }, tracker, policy);
    }

    @Test
    void handlerFailureRollsBackTheMarkBeforeTheAcknowledgment() {
        IdempotencyStore store = mock(IdempotencyStore.class);
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(true);
        Message<?> message = record();

        assertThatThrownBy(() -> new IdempotentConsumer("orders", "primary", store,
                msg -> { throw new IllegalStateException("boom"); }, tracker,
                policy(AckMode.MANUAL, AckPolicy.Owner.STARTER)).accept(message))
                .isInstanceOf(IllegalStateException.class);

        // The rollback carries the original message, not the one the handler was given.
        verify(store).rollback(eq("primary"), eq("orders"), eq(List.of(message)));
    }

    private IdempotentConsumer consumer(AckPolicy policy, Consumer<Message<?>> handler) {
        return new IdempotentConsumer("orders", "primary", IdempotencyStore.DISABLED,
                handler, tracker, policy);
    }

    private static AckPolicy policy(AckMode mode, AckPolicy.Owner owner) {
        return new AckPolicy(mode, owner, false);
    }

    private static void acknowledge(Message<?> message) {
        acknowledgment(message).acknowledge();
    }

    private static Acknowledgment acknowledgment(Message<?> message) {
        return message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
    }

    private Message<?> record() {
        return MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "order-events")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 2)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();
    }
}
