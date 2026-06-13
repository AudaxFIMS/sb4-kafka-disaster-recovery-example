package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

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

        delegate.accept(msg);
        trackTimestamp(msg);
    }

    private void trackTimestamp(Message<?> msg) {
        if (timestampTracker == null) return;
        Long timestamp = msg.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP, Long.class);
        if (timestamp != null) {
            timestampTracker.update(consumerName, timestamp);
        }
    }
}
