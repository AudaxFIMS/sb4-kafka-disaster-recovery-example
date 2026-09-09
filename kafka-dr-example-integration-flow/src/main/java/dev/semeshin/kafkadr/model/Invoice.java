package dev.semeshin.kafkadr.model;

/** What the flow produces from an {@link OrderEvent}, published to the invoices topic. */
public record Invoice(String invoiceId, String orderId, int amountDue, String customer) {

    public static Invoice from(OrderEvent order) {
        return new Invoice("inv-" + order.orderId(), order.orderId(), order.amount(), order.customer());
    }
}
