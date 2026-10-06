package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.model.OrderEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Record path, approach "flow behind the handler".
 *
 * <p>The handler is one line: it hands the message to a {@code DirectChannel} and everything
 * that follows is the flow's business. What matters is what it does <b>not</b> do — it does not
 * catch. The starter marks the record in the idempotency store before this method runs and
 * advances the watermark after it returns, so an exception escaping the flow is the only way
 * to say "this record was not processed": the mark is rolled back and Kafka redelivers.
 *
 * <p>{@code channel.send()} wraps the failure in a {@code MessagingException} whose exact
 * subtype depends on where in the flow it happened. That is fine here, because nothing
 * inspects the type — the record simply comes back. Where the type matters (the batch path),
 * use the gateway instead.
 */
@Component
public class OrderFlowProcessor implements MessageProcessor {

    private final MessageChannel ordersIn;
    private final AtomicInteger received = new AtomicInteger();

    public OrderFlowProcessor(@Qualifier("ordersIn") MessageChannel ordersIn) {
        this.ordersIn = ordersIn;
    }

    public void processOrder(Message<OrderEvent> message) {
        received.incrementAndGet();
        ordersIn.send(message);
    }

    public int receivedCount() {
        return received.get();
    }
}
