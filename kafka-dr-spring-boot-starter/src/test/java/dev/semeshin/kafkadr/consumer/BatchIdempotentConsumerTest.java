package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig.ErrorPolicy;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchIdempotentConsumerTest {

    private LastProcessedTimestampTracker tracker;
    private List<Object> processed;
    private Consumer<Message<?>> delegate;

    @BeforeEach
    void setup() {
        tracker = new LastProcessedTimestampTracker(null);
        processed = new ArrayList<>();
        delegate = msg -> processed.add(msg.getPayload());
    }

    /** Per-record driver, matching what the registry builds for a Message<T> handler. */
    private BatchHandler perRecord(ErrorPolicy policy) {
        return messages -> {
            List<Message<?>> skipped = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                try {
                    delegate.accept(messages.get(i));
                } catch (RuntimeException e) {
                    if (policy == ErrorPolicy.SKIP_FAILED) {
                        skipped.add(messages.get(i));
                        continue;
                    }
                    return BatchHandler.Result.stoppedAt(i, messages, e);
                }
            }
            return BatchHandler.Result.skipping(skipped);
        };
    }

    @Test
    void everyRecordReachesTheHandlerInPollOrder() {
        consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH).accept(batchOf("a", "b", "c"));

        assertThat(processed).containsExactly("a", "b", "c");
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    @Test
    void duplicatesAreFilteredBeforeTheHandler() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        store.tryProcess("primary", "orders", record("b", 1));

        consumer(store, ErrorPolicy.FAIL_BATCH).accept(batchOf("a", "b", "c"));

        assertThat(processed).containsExactly("a", "c");
    }

    @Test
    void batchOfOnlyDuplicatesStillAdvancesTheWatermark() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        List.of("a", "b", "c").forEach(k -> store.tryProcess("primary", "orders", record(k, 0)));

        consumer(store, ErrorPolicy.FAIL_BATCH).accept(batchOf("a", "b", "c"));

        assertThat(processed).isEmpty();
        // The container commits the batch either way, so the watermark must follow.
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
    }

    @Test
    void failBatchStopsAtTheFailingRecordAndReportsItsOriginalIndex() {
        delegate = msg -> {
            if ("b".equals(msg.getPayload())) throw new IllegalStateException("boom");
            processed.add(msg.getPayload());
        };

        assertThatThrownBy(() -> consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH)
                .accept(batchOf("a", "b", "c")))
                .isInstanceOf(BatchListenerFailedException.class)
                .satisfies(e -> assertThat(((BatchListenerFailedException) e).getIndex()).isEqualTo(1))
                .hasRootCauseMessage("boom");

        assertThat(processed).containsExactly("a");
    }

    @Test
    void failBatchLeavesTheWatermarkAloneUnderContainerAckModes() {
        delegate = msg -> {
            if ("b".equals(msg.getPayload())) throw new IllegalStateException("boom");
            processed.add(msg.getPayload());
        };

        assertThatThrownBy(() -> consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH)
                .accept(batchOf("a", "b", "c")))
                .isInstanceOf(BatchListenerFailedException.class);

        // Spring Integration wraps the exception before the container sees it, so the whole
        // batch is redelivered rather than committed up to the failure. Nothing landed, so
        // the watermark must not move — moving it would put it ahead of the commit point.
        // See BatchAcknowledgmentTest for the ack-mode that does give partial commits.
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void failBatchRollsBackMarksForTheUnprocessedTail() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        delegate = msg -> {
            if ("b".equals(msg.getPayload())) throw new IllegalStateException("boom");
            processed.add(msg.getPayload());
        };

        assertThatThrownBy(() -> consumer(store, ErrorPolicy.FAIL_BATCH).accept(batchOf("a", "b", "c")))
                .isInstanceOf(BatchListenerFailedException.class);

        // b and c were marked but never processed — a redelivery must not skip them.
        assertThat(store.tryProcess("primary", "orders", record("b", 1))).isTrue();
        assertThat(store.tryProcess("primary", "orders", record("c", 2))).isTrue();
        // a really was processed and stays marked.
        assertThat(store.tryProcess("primary", "orders", record("a", 0))).isFalse();
    }

    @Test
    void indexIsTranslatedBackToTheOriginalBatchAfterDeduplication() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        store.tryProcess("primary", "orders", record("a", 0));
        store.tryProcess("primary", "orders", record("b", 1));
        delegate = msg -> { throw new IllegalStateException("boom"); };

        assertThatThrownBy(() -> consumer(store, ErrorPolicy.FAIL_BATCH).accept(batchOf("a", "b", "c")))
                .isInstanceOf(BatchListenerFailedException.class)
                // "c" sits at index 0 of the filtered list but index 2 of the batch the
                // container holds; committing on the filtered index would lose a and b.
                .satisfies(e -> assertThat(((BatchListenerFailedException) e).getIndex()).isEqualTo(2));
    }

    @Test
    void skipFailedContinuesAndKeepsProcessingTheRest() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        delegate = msg -> {
            if ("b".equals(msg.getPayload())) throw new IllegalStateException("boom");
            processed.add(msg.getPayload());
        };

        consumer(store, ErrorPolicy.SKIP_FAILED).accept(batchOf("a", "b", "c"));

        assertThat(processed).containsExactly("a", "c");
        // Nothing threw, so the container commits the whole batch — including the skipped
        // record — and the watermark has to match that.
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1002L);
        assertThat(store.tryProcess("primary", "orders", record("b", 1))).isTrue();
    }

    @Test
    void recordModeEnvelopeIsHandledWithoutBatchWrapping() {
        // A late-initialized cluster may still deliver single records.
        Message<?> single = record("solo", 0);

        consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH).accept(single);

        assertThat(processed).containsExactly("solo");
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1000L);
    }

    @Test
    void recordModeFailureRethrowsTheOriginalExceptionNotABatchFailure() {
        RuntimeException boom = new IllegalStateException("boom");
        delegate = msg -> { throw boom; };

        assertThatThrownBy(() -> consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH)
                .accept(record("solo", 0)))
                .isSameAs(boom);
    }

    @Test
    void emptyBatchIsANoOp() {
        Message<?> empty = MessageBuilder.withPayload(List.of())
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of())
                .build();

        consumer(IdempotencyStore.DISABLED, ErrorPolicy.FAIL_BATCH).accept(empty);

        assertThat(processed).isEmpty();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    private BatchIdempotentConsumer consumer(IdempotencyStore store, ErrorPolicy policy) {
        return new BatchIdempotentConsumer("orders", "primary", store, perRecord(policy), tracker, null);
    }

    /** Batch of records on partition 0, timestamps 1000, 1001, ... keyed by payload. */
    private static Message<?> batchOf(String... payloads) {
        List<String> values = List.of(payloads);
        return BatchMessagesTest.envelope(
                values,
                values,
                values.stream().map(v -> "orders").toList(),
                values.stream().map(v -> 0).toList(),
                IntStream.range(0, values.size()).mapToObj(i -> (long) i).toList(),
                IntStream.range(0, values.size()).mapToObj(i -> 1000L + i).toList(),
                values.stream().map(v -> Map.of()).toList());
    }

    private static Message<?> record(String payload, int index) {
        return MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_KEY, payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "orders")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, (long) index)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1000L + index)
                .build();
    }
}
