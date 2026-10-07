package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bridge {@code core -> analytics}: reads {@code orders} from core and forwards each one into
 * analytics, where it is scored.
 *
 * <p>Three choices keep the bridge from losing or doubling records, and each is easy to undo
 * by accident:
 * <ul>
 *   <li>the producer is addressed by name — {@code to("order-analytics")} — so this code does
 *       not know, or care, which Kafka that is;</li>
 *   <li>the send ends in {@code orThrow()}. A failed send that returned normally would be
 *       acknowledged and lost; the exception is what lets the starter hold the record back
 *       ({@code depends-on}) or redeliver it;</li>
 *   <li>the source key travels with the record. A crash between the send and the commit
 *       sends it twice — no transaction spans two Kafkas — and the consumer in analytics
 *       drops the second copy by that key.</li>
 * </ul>
 *
 * <p>No acknowledgment code: {@code ack.owner=starter} commits once this method returns.
 */
@Component
public class OrderBridgeProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderBridgeProcessor.class);

    private final ResilientProducer producer;
    private final AtomicInteger bridged = new AtomicInteger();

    public OrderBridgeProcessor(ResilientProducer producer) {
        this.producer = producer;
    }

    public void bridgeOrder(Message<OrderEvent> message) {
        String key = IdempotencyStore.kafkaKey(message, null);
        ResilientProducer.SendResult result = producer.to("order-analytics")
                .send(message.getPayload(), key)
                .orThrow();
        log.info("Bridged order {} core -> {} ({} total)", key, result.cluster(), bridged.incrementAndGet());
    }

    public int bridgedCount() {
        return bridged.get();
    }
}
