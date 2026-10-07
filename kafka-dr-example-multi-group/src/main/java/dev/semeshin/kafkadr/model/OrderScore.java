package dev.semeshin.kafkadr.model;

/** A score computed in {@code analytics} and sent back to {@code core}. */
public record OrderScore(String orderId, String customer, int score) {

    /** A deliberately simple rule: the point is the round trip, not the scoring. */
    public static OrderScore of(OrderEvent order) {
        return new OrderScore(order.orderId(), order.customer(), Math.min(100, order.amount() / 10));
    }
}
