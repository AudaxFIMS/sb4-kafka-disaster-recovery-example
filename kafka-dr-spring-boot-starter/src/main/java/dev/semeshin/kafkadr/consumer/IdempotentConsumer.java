package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.function.Consumer;

/**
 * Wraps any message consumer with idempotency check.
 * Accepts Message<?> — the payload type is determined by the downstream handler.
 * The deduplication decision is fully delegated to the IdempotencyStore,
 * which receives the complete message (headers + payload).
 */
public class IdempotentConsumer implements Consumer<Message<?>> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final String consumerName;
    private final String clusterName;
    private final IdempotencyStore idempotencyStore;
    private final Consumer<Message<?>> delegate;
    private final LastProcessedTimestampTracker timestampTracker;

    public IdempotentConsumer(String consumerName,
                              String clusterName,
                              IdempotencyStore idempotencyStore,
                              Consumer<Message<?>> delegate,
                              LastProcessedTimestampTracker timestampTracker) {
        this.consumerName = consumerName;
        this.clusterName = clusterName;
        this.idempotencyStore = idempotencyStore;
        this.delegate = delegate;
        this.timestampTracker = timestampTracker;
    }

    @Override
    public void accept(Message<?> msg) {
        if (!idempotencyStore.tryProcess(clusterName, consumerName, msg)) {
            log.info("[{}][{}] Duplicate skipped: idempotency key={}", clusterName, consumerName, idempotencyStore.extractKey(msg));
            return;
        }

        log.info("[{}][{}] Processing: key={}", clusterName, consumerName, IdempotencyStore.kafkaKey(msg, null));

        try {
            delegate.accept(msg);
        } catch (RuntimeException e) {
            // The message was marked as processed before the handler ran. If the handler
            // failed, the mark has to go away or a redelivery would be dropped as a duplicate.
            idempotencyStore.rollback(clusterName, consumerName, List.of(msg));
            throw e;
        }
        trackTimestamp(msg);
    }

    private void trackTimestamp(Message<?> msg) {
        if (timestampTracker == null) return;
        if (!timestampTracker.advance(msg)) {
            log.debug("[{}][{}] Missing topic/partition/timestamp headers, watermark not advanced",
                    clusterName, consumerName);
        }
    }
}
