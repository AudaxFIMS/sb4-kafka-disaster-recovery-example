package dev.semeshin.kafkadr.idempotency;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Tracks processed messages to prevent duplicate processing during DR failover.
 * Entries are scoped by consumer name to isolate different consumers/topics.
 *
 * <p>Implementations receive the full message and may derive the deduplication
 * key from any headers or payload data. Override {@link #extractKey(Message)}
 * to customize key extraction only, keeping the storage logic of the base
 * implementation intact.
 */
public interface IdempotencyStore {

    /**
     * No-op store used when kafka-dr.idempotency.enabled=false: every message
     * is processed, nothing is tracked. Lets IdempotentConsumer stay in the
     * consumer chain (it still feeds the last-processed-timestamp tracker
     * required for seek-by-timestamp failover).
     */
    IdempotencyStore DISABLED = new IdempotencyStore() {
        @Override
        public boolean tryProcess(String clusterName, String consumerName, Message<?> message) {
            return true;
        }

        @Override
        public String extractKey(Message<?> message) {
            return null;
        }

        @Override
        public List<Message<?>> filterProcessable(String clusterName, String consumerName,
                                                  List<Message<?>> messages) {
            return messages;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    };

    /**
     * Whether deduplication is actually performed. The {@link #DISABLED} no-op
     * returns false so callers can skip key extraction and dedup-related logging.
     */
    default boolean isEnabled() {
        return true;
    }

    /**
     * Attempts to mark a message as processed for a given consumer.
     *
     * @param clusterName logical cluster name (e.g. "primary" , "secondary" ...)
     * @param consumerName logical consumer name (e.g. "orders-consumer", "payments-consumer")
     * @param message      full message — headers and payload — to derive the deduplication key from
     * @return true if the message should be processed (not seen before), false if duplicate
     */
    boolean tryProcess(String clusterName, String consumerName, Message<?> message);

    /**
     * Batch counterpart of {@link #tryProcess}: returns the messages that should be
     * processed, in their original order.
     *
     * <p>The default implementation loops, so every existing store works in batch mode
     * unchanged. Implementations backed by a remote store should override it — a Redis
     * pipeline turns 500 sequential round-trips into one.
     *
     * <p>The returned list must contain <b>the same message instances</b> as the input,
     * not copies: callers map records back to their position in the batch by identity,
     * which is what makes partial commits land on the right offset.
     *
     * @param clusterName  logical cluster name
     * @param consumerName logical consumer name
     * @param messages     the batch, in poll order
     * @return the subset to process, in the same relative order
     */
    default List<Message<?>> filterProcessable(String clusterName, String consumerName,
                                               List<Message<?>> messages) {
        List<Message<?>> accepted = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            if (tryProcess(clusterName, consumerName, message)) {
                accepted.add(message);
            }
        }
        return accepted;
    }

    /**
     * Removes the marks left by {@link #tryProcess} for messages that were accepted
     * but never actually processed — the handler threw, or a batch was abandoned
     * part-way through.
     *
     * <p>Without this, the mark-then-process order is at-most-once: a failure between
     * the two steps leaves the message recorded as done, and the redelivery Kafka
     * performs is dropped as a duplicate. The window is narrow with auto-commit and
     * as wide as the application wants it with manual acknowledgment.
     *
     * <p>Default is a no-op so existing stores keep compiling; implementations that
     * can delete their keys should override it.
     *
     * @param clusterName  logical cluster name
     * @param consumerName logical consumer name
     * @param messages     messages to un-mark
     */
    default void rollback(String clusterName, String consumerName, List<Message<?>> messages) {
    }

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
     * @param message      full message — headers and payload
     * @return the key, or null if the message has no key (built-in stores then
     *         process the message without idempotency check)
     */
    default String extractKey(Message<?> message) {
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
