package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.avro.PaymentEvent;
import dev.semeshin.kafkadr.consumer.MessageProcessor;
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
public class PaymentAndRawDataMessageProcessor implements MessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(PaymentAndRawDataMessageProcessor.class);

    public void processPayment(Message<PaymentEvent> message) {
        PaymentEvent payment = message.getPayload();
        log.info("[payment-events] messageId={}, paymentId={}", message.getHeaders().get("message-id"), payment.getPaymentId());
    }

    public void processRawData(Message<byte[]> message) {
        byte[] data = message.getPayload();
        log.info("[raw-telemetry] messageId={}, size={} bytes", message.getHeaders().get("message-id"), data.length);
    }
}
