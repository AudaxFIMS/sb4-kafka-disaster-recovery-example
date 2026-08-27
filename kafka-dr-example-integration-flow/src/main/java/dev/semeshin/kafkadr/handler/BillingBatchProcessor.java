package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.BatchOutcome;
import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.flow.BillingGateway;
import dev.semeshin.kafkadr.model.Invoice;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.model.UnprocessableOrderException;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Batch path: a flow per record, one publish for the whole batch.
 *
 * <p>The loop lives here rather than inside the flow because {@link BatchOutcome} is indexed:
 * splitting the batch inside the flow would lose the position of each record, and with it the
 * ability to say which prefix may be committed. Each record goes through the gateway, whose
 * reply is the invoice to publish.
 *
 * <p>The tail is a single {@link ResilientProducer#sendBatch}: one failover decision for
 * everything the batch produced, instead of re-running the retry ladder per invoice against a
 * cluster that may already be gone.
 */
@Component
public class BillingBatchProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(BillingBatchProcessor.class);

    private final BillingGateway gateway;
    private final ResilientProducer producer;
    private final AtomicInteger batches = new AtomicInteger();
    private final AtomicInteger billed = new AtomicInteger();
    private final AtomicInteger discarded = new AtomicInteger();

    public BillingBatchProcessor(BillingGateway gateway, ResilientProducer producer) {
        this.gateway = gateway;
        this.producer = producer;
    }

    public BatchOutcome billOrders(List<Message<OrderEvent>> messages) {
        log.info("Billing batch #{}: {} orders", batches.incrementAndGet(), messages.size());

        BatchOutcome outcome = BatchOutcome.of(messages);
        List<Message<?>> invoices = new ArrayList<>(messages.size());

        for (int i = 0; i < messages.size(); i++) {
            Message<OrderEvent> message = messages.get(i);
            OrderEvent order = message.getPayload();
            try {
                // Rejected before the gateway on purpose: billingFlow expects a reply, and a
                // filter inside it would leave the call waiting for a reply that never comes.
                if (order.amount() < 0) {
                    throw new UnprocessableOrderException("negative amount " + order.amount());
                }

                Invoice invoice = gateway.bill(message);
                invoices.add(MessageBuilder.withPayload(invoice)
                        .setHeader(KafkaHeaders.KEY, invoice.invoiceId())
                        .build());
                outcome.done(i);
                billed.incrementAndGet();
            } catch (UnprocessableOrderException e) {
                // The gateway rethrows the original exception, so this typed catch matches.
                // With channel.send() it would arrive as MessageDeliveryException, miss this
                // branch, and the record would be retried forever instead of discarded.
                log.warn("  {} discarded: {}", order.orderId(), e.getMessage());
                outcome.discard(i, e);
                discarded.incrementAndGet();
            } catch (RuntimeException e) {
                // Anything else is treated as transient: this record and the rest of the batch
                // come back. Everything before it stays committed.
                log.warn("  {} handed back: {}", order.orderId(), e.toString());
                outcome.retry(i, e);
                break;
            }
        }

        publish(invoices);
        return outcome;
    }

    /**
     * Publishing before returning the outcome is deliberate: the starter acknowledges the
     * batch based on what this method reports, so a failed publish has to be visible as a
     * failure of the batch, not as a silently lost invoice.
     */
    private void publish(List<Message<?>> invoices) {
        if (invoices.isEmpty()) {
            return;
        }
        ResilientProducer.BatchSendResult result = producer.sendBatch("flow-invoices", invoices);
        log.info("  published {} of {} invoices to {}", result.sent(), result.size(), result.clusters());
        if (!result.allSent()) {
            throw new IllegalStateException(
                    "Only %d of %d invoices were published".formatted(result.sent(), result.size()));
        }
    }

    public int batchCount() { return batches.get(); }
    public int billedCount() { return billed.get(); }
    public int discardedCount() { return discarded.get(); }
}
