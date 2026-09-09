package dev.semeshin.kafkadr.model;

/**
 * @param orderId  doubles as the Kafka record key and the deduplication key
 * @param amount   zero is filtered out by the flow; negative is unprocessable
 * @param customer free-form payload field
 */
public record OrderEvent(String orderId, int amount, String customer) {
}
