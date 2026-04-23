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
 */
public class IdempotentConsumer implements Consumer<Message<?>> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final String consumerName;
    private final String clusterName;
    private final IdempotencyStore idempotencyStore;
    private final Consumer<Message<?>> delegate;

    public IdempotentConsumer(String consumerName,
                              String clusterName,
                              IdempotencyStore idempotencyStore,
                              Consumer<Message<?>> delegate) {
        this.consumerName = consumerName;
        this.clusterName = clusterName;
        this.idempotencyStore = idempotencyStore;
        this.delegate = delegate;
    }

    @Override
    public void accept(Message<?> msg) {
        String key = extractKey(msg);

        if (key == null) {
            log.warn("[{}@{}] Message without key, processing without idempotency check",
                    consumerName, clusterName);
            delegate.accept(msg);
            return;
        }

        if (!idempotencyStore.tryProcess(consumerName, key)) {
            log.info("[{}@{}] Duplicate skipped: key={}", consumerName, clusterName, key);
            return;
        }

        log.info("[{}@{}] Processing: key={}", consumerName, clusterName, key);
        delegate.accept(msg);
    }

    private String extractKey(Message<?> msg) {
        // Try kafka_receivedMessageKey (consumer side) first, then kafka_messageKey (producer side)
        Object key = msg.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
        if (key == null) {
            key = msg.getHeaders().get(KafkaHeaders.KEY);
        }
        return key != null ? key.toString() : null;
    }
}
