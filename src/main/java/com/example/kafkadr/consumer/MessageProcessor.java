package com.example.kafkadr.consumer;

import com.example.kafkadr.avro.PaymentEvent;
import com.example.kafkadr.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    /** content-type: string — payload arrives as String */
    public void processDemoEvent(Message<String> message) {
        log.info("[demo-events] messageId={}", message.getHeaders().get("message-id"));
    }

    /** content-type: json — payload deserialized from JSON via Jackson */
    public void processOrder(Message<OrderEvent> message) {
        OrderEvent order = message.getPayload();
        log.info("[order-events] messageId={}, orderId={}", message.getHeaders().get("message-id"), order.getOrderId());
    }

    /** content-type: native — payload deserialized by KafkaAvroDeserializer */
    public void processPayment(Message<PaymentEvent> message) {
        PaymentEvent payment = message.getPayload();
        log.info("[payment-events] messageId={}, paymentId={}", message.getHeaders().get("message-id"), payment.getPaymentId());
    }

    /** content-type: bytes — raw byte[] payload, no conversion */
    public void processRawData(Message<byte[]> message) {
        byte[] data = message.getPayload();
        log.info("[raw-telemetry] messageId={}, size={} bytes", message.getHeaders().get("message-id"), data.length);
    }
}
