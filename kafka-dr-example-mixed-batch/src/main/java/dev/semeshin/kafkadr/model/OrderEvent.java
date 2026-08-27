package dev.semeshin.kafkadr.model;

/**
 * @param orderId  doubles as the Kafka record key and the deduplication key
 * @param amount   negative values are treated as unprocessable, to demonstrate discard
 * @param customer free-form payload field
 */
public record OrderEvent(String orderId, int amount, String customer) {
}
