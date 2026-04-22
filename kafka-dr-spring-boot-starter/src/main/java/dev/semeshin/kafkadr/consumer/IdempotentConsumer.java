package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        String messageId = msg.getHeaders().get("message-id", String.class);

        if (messageId == null) {
            log.warn("[{}@{}] Message without message-id, processing anyway",
                    consumerName, clusterName);
            delegate.accept(msg);
            return;
        }

        if (!idempotencyStore.tryProcess(consumerName, messageId)) {
            log.info("[{}@{}] Duplicate skipped: messageId={}", consumerName, clusterName, messageId);
            return;
        }

        log.info("[{}@{}] Processing: messageId={}", consumerName, clusterName, messageId);
        delegate.accept(msg);
    }
}
