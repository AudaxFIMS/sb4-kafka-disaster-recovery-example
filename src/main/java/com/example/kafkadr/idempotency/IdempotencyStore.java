package com.example.kafkadr.idempotency;

/**
 * Tracks processed message IDs to prevent duplicate processing during DR failover.
 * Keys are scoped by consumer name to isolate different consumers/topics.
 */
public interface IdempotencyStore {

    /**
     * Attempts to mark a message as processed for a given consumer.
     *
     * @param consumerName logical consumer name (e.g. "orders-consumer", "payments-consumer")
     * @param messageId    unique message identifier
     * @return true if the message was NOT seen before (first time), false if duplicate
     */
    boolean tryProcess(String consumerName, String messageId);
}
