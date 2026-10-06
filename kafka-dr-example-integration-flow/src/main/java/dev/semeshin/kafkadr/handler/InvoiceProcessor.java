package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.model.Invoice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes what the flows published, so the round trip through Kafka is visible in
 * {@code GET /api/status} instead of only in the logs. An ordinary record consumer — no flow
 * involved, to keep the contrast with the two that do use one.
 */
@Component
public class InvoiceProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(InvoiceProcessor.class);

    private final AtomicInteger consumed = new AtomicInteger();

    public void processInvoice(Message<Invoice> message) {
        Invoice invoice = message.getPayload();
        log.info("Invoice consumed: {} for order {} ({} total)",
                invoice.invoiceId(), invoice.orderId(), consumed.incrementAndGet());
    }

    public int consumedCount() {
        return consumed.get();
    }
}
