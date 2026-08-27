package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.BatchOutcome.Verdict;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.Shape;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchOutcomeTest {

    // --- the value type -------------------------------------------------------

    @Test
    void unmarkedRecordsCountAsRetry() {
        BatchOutcome outcome = BatchOutcome.of(messages(3)).done(0);

        // Assuming success would silently drop whatever the handler forgot; assuming
        // failure costs a redelivery the idempotency store absorbs.
        assertThat(outcome.verdictAt(1)).isEqualTo(Verdict.RETRY);
        assertThat(outcome.unmarkedCount()).isEqualTo(2);
        assertThat(outcome.firstRetryIndex()).isEqualTo(1);
    }

    @Test
    void discardIsNotRetriedAndKeepsItsMark() {
        BatchOutcome outcome = BatchOutcome.of(messages(3))
                .done(0)
                .discard(1, new IllegalArgumentException("poison"))
                .done(2);

        // discard means "closed for good": releasing the mark would send it back through
        // the handler on the next redelivery, where it would fail again.
        assertThat(outcome.firstRetryIndex()).isEqualTo(-1);
        assertThat(outcome.retried()).isEmpty();
    }

    @Test
    void retriedRecordsAreReportedRegardlessOfPosition() {
        List<Message<?>> batch = messages(5);
        BatchOutcome outcome = BatchOutcome.of(batch)
                .done(0)
                .retry(1, new IllegalStateException("later"))
                .done(2)
                .retry(3, new IllegalStateException("later"))
                .done(4);

        assertThat(outcome.firstRetryIndex()).isEqualTo(1);
        assertThat(outcome.retried()).containsExactly(batch.get(1), batch.get(3));
    }

    @Test
    void markingOutsideTheBatchFails() {
        BatchOutcome outcome = BatchOutcome.of(messages(2));

        assertThatThrownBy(() -> outcome.done(2)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    // --- driven through the registry -----------------------------------------

    static class Handlers implements MessageProcessor {
        BiConsumer<List<Message<Object>>, BatchOutcome> behaviour = (messages, outcome) -> {
            for (int i = 0; i < messages.size(); i++) {
                outcome.done(i);
            }
        };

        public BatchOutcome withVerdicts(List<Message<Object>> messages) {
            BatchOutcome outcome = BatchOutcome.of(messages);
            behaviour.accept(messages, outcome);
            return outcome;
        }
    }

    @Test
    void batchOutcomeHandlerIsRecognisedByItsReturnType() {
        assertThat(registry(new Handlers()).shapeOf("c")).isEqualTo(Shape.BATCH_OUTCOME);
    }

    @Test
    void onlyRetriedRecordsAreRolledBack() {
        Handlers handlers = new Handlers();
        handlers.behaviour = (messages, outcome) -> outcome
                .done(0)
                .retry(1, new IllegalStateException("transient"))
                .done(2);

        BatchHandler.Result result = registry(handlers).getBatchHandler("c").process(messages(3));

        assertThat(result.firstUnprocessedIndex()).isEqualTo(1);
        assertThat(result.rollback()).hasSize(1);
        assertThat(result.failure()).hasMessageContaining("transient");
    }

    @Test
    void aRecordDoneAfterARetriedOneKeepsItsMarkAndIsDeduplicated() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        Handlers handlers = new Handlers();
        handlers.behaviour = (messages, outcome) -> outcome
                .done(0)
                .retry(1, new IllegalStateException("transient"))
                .done(2);

        List<Message<?>> batch = messages(3);
        BatchHandler handler = registry(handlers).getBatchHandler("c");
        List<Message<?>> accepted = store.filterProcessable("primary", "c", batch);
        BatchHandler.Result result = handler.process(accepted);

        store.rollback("primary", "c", result.rollback());

        // Offsets commit as a watermark, so records 1 and 2 both come back. Record 2 was
        // processed, and the store is what remembers that.
        assertThat(store.tryProcess("primary", "c", batch.get(1))).isTrue();
        assertThat(store.tryProcess("primary", "c", batch.get(2))).isFalse();
    }

    @Test
    void allDoneCommitsTheWholeBatch() {
        BatchHandler.Result result = registry(new Handlers()).getBatchHandler("c").process(messages(3));

        assertThat(result).isEqualTo(BatchHandler.Result.COMPLETE);
    }

    @Test
    void handlerThrowingBeforeReportingRestartsTheBatch() {
        Handlers handlers = new Handlers();
        handlers.behaviour = (messages, outcome) -> { throw new IllegalStateException("blew up"); };

        BatchHandler.Result result = registry(handlers).getBatchHandler("c").process(messages(3));

        assertThat(result.firstUnprocessedIndex()).isZero();
        assertThat(result.rollback()).hasSize(3);
    }

    // --- fixtures -------------------------------------------------------------

    private static MessageHandlerRegistry registry(Handlers handlers) {
        ConsumerConfig config = new ConsumerConfig();
        config.setName("c");
        config.setTopic("orders");
        config.setHandler("withVerdicts");
        config.setContentType("bytes");
        BatchConfig batch = new BatchConfig();
        batch.setEnabled(true);
        config.setBatch(batch);

        KafkaClusterProperties properties = new KafkaClusterProperties();
        Map<String, ConsumerConfig> map = new LinkedHashMap<>();
        map.put("c", config);
        properties.setConsumers(map);
        return new MessageHandlerRegistry(List.of(handlers), properties);
    }

    private static List<Message<?>> messages(int size) {
        List<Message<?>> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messages.add(MessageBuilder.withPayload("p" + i)
                    .setHeader(KafkaHeaders.RECEIVED_KEY, "k" + i)
                    .build());
        }
        return messages;
    }
}
