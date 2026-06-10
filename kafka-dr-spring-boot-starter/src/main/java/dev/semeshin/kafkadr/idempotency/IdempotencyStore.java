package dev.semeshin.kafkadr.idempotency;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.nio.charset.StandardCharsets;

/**
 * Tracks processed messages to prevent duplicate processing during DR failover.
 * Entries are scoped by consumer name to isolate different consumers/topics.
 *
 * <p>Implementations receive the full message and may derive the deduplication
 * key from any headers or payload data. Override {@link #extractKey(String, Message)}
 * to customize key extraction only, keeping the storage logic of the base
 * implementation intact.
 */
public interface IdempotencyStore {

    /**
     * Attempts to mark a message as processed for a given consumer.
     *
     * @param consumerName logical consumer name (e.g. "orders-consumer", "payments-consumer")
     * @param message      full message — headers and payload — to derive the deduplication key from
     * @return true if the message should be processed (not seen before), false if duplicate
     */
    boolean tryProcess(String consumerName, Message<?> message);

    /**
     * Extracts the deduplication key for a message. The built-in implementations
     * call this from {@code tryProcess}, so overriding this method customizes key
     * extraction (any header, payload data, per-consumer logic) without rewriting
     * the storage logic.
     *
     * <p>Default: the Kafka record key — {@code KafkaHeaders.RECEIVED_KEY}
     * (consumer side), then {@code KafkaHeaders.KEY} (producer side);
     * {@code byte[]} keys are converted to UTF-8 strings.
     *
     * @param consumerName logical consumer name, for per-consumer extraction logic
     * @param message      full message — headers and payload
     * @return the key, or null if the message has no key (built-in stores then
     *         process the message without idempotency check)
     */
    default String extractKey(String consumerName, Message<?> message) {
        return kafkaKey(message, null);
    }

    /**
     * Default key-based extraction, shared by the built-in implementations.
     *
     * <p>If {@code customKeyHeader} is set, that header is used exclusively.
     * Otherwise falls back to the Kafka record key: {@code KafkaHeaders.RECEIVED_KEY}
     * (consumer side), then {@code KafkaHeaders.KEY} (producer side).
     * {@code byte[]} keys are converted to UTF-8 strings.
     *
     * @return the key, or null if the message has no key
     */
    static String kafkaKey(Message<?> message, String customKeyHeader) {
        Object key;
        if (customKeyHeader != null && !customKeyHeader.isBlank()) {
            key = message.getHeaders().get(customKeyHeader);
        } else {
            key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
            if (key == null) {
                key = message.getHeaders().get(KafkaHeaders.KEY);
            }
        }
        if (key == null) return null;
        if (key instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        return key.toString();
    }
}
