package dev.semeshin.kafkadr.model;

/** An order, published to {@code core} and bridged to {@code analytics}. */
public record OrderEvent(String orderId, int amount, String customer) {}
