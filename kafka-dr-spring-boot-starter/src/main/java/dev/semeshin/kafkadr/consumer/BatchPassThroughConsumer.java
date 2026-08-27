package dev.semeshin.kafkadr.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

/**
 * Batch consumer for {@code batch.mode: standard} — the handler receives the raw
 * {@code Message<List<T>>} envelope, exactly as plain Spring Cloud Stream delivers it,
 * including {@code kafka_acknowledgment} and {@code kafka_batchConvertedHeaders}.
 *
 * <p>Nothing is deduplicated here: without per-record messages there is no key to look
 * up, so the DR guarantee drops to "the whole batch is redelivered". Exceptions are not
 * caught either — error handling belongs entirely to the handler, which is what makes
 * this mode behave like ordinary Spring Cloud Stream.
 *
 * <p>The watermark is the maximum timestamp per partition across the batch, advanced only
 * after the handler returns normally. It is necessarily coarser than in split mode: the
 * starter cannot know which records the handler actually completed.
 */
public class BatchPassThroughConsumer implements Consumer<Message<?>> {

    private static final Logger log = LoggerFactory.getLogger(BatchPassThroughConsumer.class);

    private final String consumerName;
    private final String clusterName;
    private final Consumer<Message<?>> delegate;
    private final LastProcessedTimestampTracker timestampTracker;

    public BatchPassThroughConsumer(String consumerName,
                                    String clusterName,
                                    Consumer<Message<?>> delegate,
                                    LastProcessedTimestampTracker timestampTracker) {
        this.consumerName = consumerName;
        this.clusterName = clusterName;
        this.delegate = delegate;
        this.timestampTracker = timestampTracker;
    }

    @Override
    public void accept(Message<?> envelope) {
        boolean batch = BatchMessages.isBatch(envelope);
        if (batch) {
            log.info("[{}][{}] Batch of {} passed through",
                    clusterName, consumerName, ((java.util.List<?>) envelope.getPayload()).size());
        }

        delegate.accept(envelope);

        if (timestampTracker == null) {
            return;
        }
        if (batch) {
            for (BatchMessages.RecordRef ref : BatchMessages.coordinates(envelope)) {
                timestampTracker.update(ref.topic(), ref.partition(), ref.timestamp());
            }
        } else {
            timestampTracker.advance(envelope);
        }
    }
}
