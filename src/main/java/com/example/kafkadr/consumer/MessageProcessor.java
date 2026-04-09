package com.example.kafkadr.consumer;

import com.example.kafkadr.avro.PaymentEvent;
import com.example.kafkadr.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    /** content-type: string — payload arrives as String */
    public void processDemoEvent(Message<String> message) {
        String text = message.getPayload();
        log.info("[demo-events] text={}", text);
    }

    /** content-type: json — payload deserialized from JSON via Jackson */
    public void processOrder(Message<OrderEvent> message) {
        OrderEvent order = message.getPayload();
        log.info("[order-events] order={}", order);
    }

    /** content-type: native — payload deserialized by KafkaAvroDeserializer */
    public void processPayment(Message<PaymentEvent> message) {
        PaymentEvent payment = message.getPayload();
        log.info("[payment-events] paymentId={}, orderId={}, amount={} {}, status={}",
                payment.getPaymentId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus());
    }

    /** content-type: bytes — raw byte[] payload, no conversion */
    public void processRawData(Message<byte[]> message) {
        byte[] data = message.getPayload();
        log.info("[raw-telemetry] received {} bytes", data.length);
    }
}
