package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * How acknowledgment mode drives the commit point, and how the watermark follows it.
 *
 * <p>The invariant under test: the watermark never passes an offset that was not
 * committed. Breaking it means a failover seeks past records that will never be
 * redelivered.
 */
class BatchAcknowledgmentTest {

    private LastProcessedTimestampTracker tracker;
    private Acknowledgment ack;

    @BeforeEach
    void setup() {
        tracker = new LastProcessedTimestampTracker(null);
        ack = mock(Acknowledgment.class);
    }

    @Test
    void manualImmediateCommitsTheWholeBatchWhenNothingFailed() {
        consumer(AckMode.MANUAL_IMMEDIATE, complete()).accept(batchOf(3));

        verify(ack).acknowledge();
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    @Test
    void manualImmediateCommitsThePrefixAndStopsTheWatermarkThere() {
        assertThatThrownBy(() -> consumer(AckMode.MANUAL_IMMEDIATE, stopsAt(2)).accept(batchOf(4)))
                .isInstanceOf(BatchListenerFailedException.class);

        // Records 0 and 1 are committed; 2 and 3 come back.
        verify(ack).acknowledge(1);
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1001L);
    }

    @Test
    void thrownExceptionCarriesThePositionTheErrorHandlerNeeds() {
        // The index, not the message text, is what DefaultErrorHandler uses to decide
        // where redelivery resumes when the container owns the commit.
        BatchListenerFailedException thrown = (BatchListenerFailedException)
                org.assertj.core.api.Assertions.catchThrowable(
                        () -> consumer(null, stopsAt(2)).accept(batchOf(4)));

        assertThat(thrown.getIndex()).isEqualTo(2);
    }

    @Test
    void manualCannotCommitPartOfABatchSoNothingMoves() {
        assertThatThrownBy(() -> consumer(AckMode.MANUAL, stopsAt(2)).accept(batchOf(4)))
                .isInstanceOf(BatchListenerFailedException.class);

        // Partial acknowledgment needs MANUAL_IMMEDIATE. Advancing the watermark for the
        // successful prefix here would put it ahead of the last committed offset.
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void manualCommitsTheWholeBatchWhenNothingFailed() {
        consumer(AckMode.MANUAL, complete()).accept(batchOf(3));

        verify(ack).acknowledge();
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    @Test
    void containerModesReplayTheWholeBatchSoTheWatermarkStaysPut() {
        assertThatThrownBy(() -> consumer(null, stopsAt(2)).accept(batchOf(4)))
                .isInstanceOf(BatchListenerFailedException.class);

        // Verified against a live broker: Spring Integration wraps the exception before
        // the container sees it, so DefaultErrorHandler logs "Expected a
        // BatchListenerFailedException; re-delivering full batch" and replays from record
        // 0. Nothing was committed, so the watermark must not move for the prefix either.
        verifyNoInteractions(ack);
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void scheduledCommitModesLeaveTheWatermarkAlone() {
        for (AckMode mode : List.of(AckMode.TIME, AckMode.COUNT, AckMode.COUNT_TIME)) {
            tracker = new LastProcessedTimestampTracker(null);
            consumer(mode, complete()).accept(batchOf(3));

            // These commit on their own schedule, which the starter cannot observe, so
            // seek-by-timestamp falls back to committed offsets instead of guessing.
            assertThat(tracker.getAllTimestamps()).as("ack-mode=%s", mode).isEmpty();
        }
    }

    @Test
    void recordModeBehavesLikeBatchBecauseTheBinderDropsIt() {
        consumer(AckMode.RECORD, complete()).accept(batchOf(3));

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    @Test
    void missingAcknowledgmentHeaderIsReportedRatherThanAssumed() {
        Message<?> withoutAck = BatchMessagesTest.envelope(
                List.of("a"), List.of("a"), List.of("orders"), List.of(0),
                List.of(0L), List.of(1000L), List.of(Map.of()));

        new BatchIdempotentConsumer("orders", "primary", IdempotencyStore.DISABLED,
                complete(), tracker, AckMode.MANUAL_IMMEDIATE).accept(withoutAck);

        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void duplicateOnlyBatchIsStillAcknowledged() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        IntStream.range(0, 3).forEach(i -> store.tryProcess("primary", "orders", record(i)));

        new BatchIdempotentConsumer("orders", "primary", store, complete(), tracker,
                AckMode.MANUAL_IMMEDIATE).accept(batchOf(3));

        // Nothing to do, but the offsets must still move or the batch is polled forever.
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    // --- fixtures -------------------------------------------------------------

    private BatchIdempotentConsumer consumer(AckMode ackMode, BatchHandler handler) {
        return new BatchIdempotentConsumer("orders", "primary", IdempotencyStore.DISABLED,
                handler, tracker, ackMode);
    }

    private static BatchHandler complete() {
        return messages -> BatchHandler.Result.COMPLETE;
    }

    private static BatchHandler stopsAt(int index) {
        return messages -> BatchHandler.Result.stoppedAt(index, messages, new IllegalStateException("boom"));
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

    private static Message<?> record(int index) {
        return MessageBuilder.withPayload("p" + index)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "k" + index)
                .build();
    }
}
