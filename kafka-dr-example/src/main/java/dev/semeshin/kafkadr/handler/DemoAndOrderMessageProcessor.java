package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Example application-level message processor.
 * Implements MessageProcessor and defines handler methods referenced
 * by name in kafka-dr.consumers[].handler configuration.
 */
@Component
public class DemoAndOrderMessageProcessor implements MessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(DemoAndOrderMessageProcessor.class);

    public void processDemoEvent(Message<String> message) {
        log.info("[demo-events] key={}", IdempotencyStore.kafkaKey(message, null));
    }

    public void processOrder(Message<OrderEvent> message) {
        OrderEvent order = message.getPayload();
        log.info("[order-events] key={}, orderId={}", IdempotencyStore.kafkaKey(message, null), order.getOrderId());
    }
}
