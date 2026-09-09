package dev.semeshin.kafkadr.flow;

import dev.semeshin.kafkadr.model.Invoice;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;

import java.nio.charset.StandardCharsets;

/**
 * The two flows this example is about.
 *
 * <p><b>Every channel here is a {@link DirectChannel} on purpose.</b> A direct channel runs its
 * subscriber on the calling thread and lets exceptions travel back up the stack, which is the
 * whole contract the starter depends on: it deduplicates before the handler, advances the
 * seek-by-timestamp watermark after it, and treats a thrown exception as "not processed".
 * A {@code QueueChannel}, an {@code ExecutorChannel} or an {@code .executor(...)} anywhere in
 * these flows would return control immediately — the offset would commit and the watermark
 * would advance while the message is still in a queue, and a later failure would be invisible
 * and unrecoverable.
 *
 * <p>For the same reason neither flow installs an {@code errorChannel}: swallowing the error
 * there would take away the starter's only signal that the record needs to come back.
 */
@Configuration
public class OrderFlowConfig {

    private static final Logger log = LoggerFactory.getLogger(OrderFlowConfig.class);

    @Bean
    public MessageChannel ordersIn() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel billingIn() {
        return new DirectChannel();
    }

    /**
     * Record path, fire-and-forget: filter → transform → publish.
     *
     * <p>The tail is {@link ResilientProducer}, not {@code Kafka.outboundChannelAdapter}. The
     * adapter is bound to a single {@code ProducerFactory}, so it would keep writing to a dead
     * cluster after a failover; the producer here picks the active cluster, retries, and forces
     * the failover when the cluster is gone.
     *
     * <p>A filtered-out message is a <b>success</b>: no exception, the offset commits and the
     * watermark advances. That is intended — the record was consumed deliberately. Adding
     * {@code throwExceptionOnRejection(true)} would turn routine filtering into endless
     * redelivery.
     */
    @Bean
    public IntegrationFlow orderFlow(ResilientProducer producer, FlowMetrics metrics) {
        return IntegrationFlow.from("ordersIn")
                .wireTap(f -> f.handle(m -> metrics.entered()))
                .<OrderEvent>filter(order -> {
                    boolean billable = order.amount() > 0;
                    if (!billable) {
                        metrics.filtered();
                        log.info("Filtered out {}: amount={}", order.orderId(), order.amount());
                    }
                    return billable;
                })
                .<OrderEvent, Invoice>transform(Invoice::from)
                .enrichHeaders(h -> h.header("x-produced-by", "orderFlow"))
                .handle(Invoice.class, (invoice, headers) -> {
                    // Synchronous by design: the send result is part of this record's outcome.
                    var result = producer.send("flow-invoices", invoice, invoice.invoiceId());
                    if (!result.success()) {
                        // Propagates through the DirectChannel, out of the handler, into the
                        // starter — the idempotency mark is rolled back and Kafka redelivers.
                        throw new IllegalStateException(
                                "No cluster accepted invoice " + invoice.invoiceId());
                    }
                    metrics.invoicePublished();
                    // The inbound key is a byte[] — the binder hands the raw Kafka key through,
                    // so it needs decoding before it is fit to print or reuse.
                    Object key = headers.get(KafkaHeaders.RECEIVED_KEY);
                    log.info("Published {} to cluster {} (source key={})", invoice.invoiceId(),
                            result.cluster(),
                            key instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : key);
                    return null;   // terminal step
                })
                .get();
    }

    /**
     * Batch path, request/reply: the flow only transforms and hands the result back, so the
     * batch handler can decide per record what to report and can publish everything it
     * produced with a single {@code sendBatch}.
     */
    @Bean
    public IntegrationFlow billingFlow() {
        return IntegrationFlow.from("billingIn")
                .<OrderEvent, Invoice>transform(Invoice::from)
                .get();
    }
}
