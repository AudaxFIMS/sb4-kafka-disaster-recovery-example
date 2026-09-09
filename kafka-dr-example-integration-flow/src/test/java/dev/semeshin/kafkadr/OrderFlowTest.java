package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.flow.BillingGateway;
import dev.semeshin.kafkadr.flow.FlowMetrics;
import dev.semeshin.kafkadr.flow.OrderFlowConfig;
import dev.semeshin.kafkadr.model.Invoice;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.model.UnprocessableOrderException;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The properties the whole example rests on, checked against a real Spring Integration
 * context: the flow runs on the calling thread, failures come back out, and a gateway
 * reports the original exception where {@code send()} reports a wrapped one.
 */
class OrderFlowTest {

    private AnnotationConfigApplicationContext ctx;
    private ResilientProducer producer;
    private MessageChannel ordersIn;
    private FlowMetrics metrics;

    @BeforeEach
    void setup() {
        producer = mock(ResilientProducer.class);
        TestConfig.PRODUCER.set(producer);
        ctx = new AnnotationConfigApplicationContext(TestConfig.class);
        ordersIn = ctx.getBean("ordersIn", MessageChannel.class);
        metrics = ctx.getBean(FlowMetrics.class);
    }

    @AfterEach
    void tearDown() {
        ctx.close();
    }

    private static Message<OrderEvent> order(int amount) {
        return MessageBuilder.withPayload(new OrderEvent("o-1", amount, "acme")).build();
    }

    @Test
    void billableOrderIsTransformedAndPublishedThroughTheResilientProducer() {
        when(producer.send(eq("flow-invoices"), any(), anyString()))
                .thenReturn(new ResilientProducer.SendResult(true, "primary", "inv-o-1"));

        ordersIn.send(order(100));

        verify(producer).send(eq("flow-invoices"), any(Invoice.class), eq("inv-o-1"));
        assertThat(metrics.getInvoicesPublished()).isEqualTo(1);
        assertThat(metrics.getFiltered()).isZero();
    }

    @Test
    void filteredOrderIsASuccessNotAFailure() {
        // No exception and no publish: the record was consumed deliberately, so the offset
        // may commit and the watermark may advance.
        ordersIn.send(order(0));

        verify(producer, never()).send(anyString(), any(), anyString());
        assertThat(metrics.getFiltered()).isEqualTo(1);
    }

    @Test
    void aFailedPublishPropagatesOutOfTheFlow() {
        // This is what lets the starter roll back the idempotency mark and let Kafka redeliver.
        when(producer.send(eq("flow-invoices"), any(), anyString()))
                .thenReturn(new ResilientProducer.SendResult(false, null, "inv-o-1"));

        Message<OrderEvent> message = order(100);

        // MessagingException, not one specific subtype: a failure in the channel's immediate
        // subscriber arrives as MessageDeliveryException, one further down the flow as
        // MessageHandlingException. Both are RuntimeExceptions, which is all the starter
        // needs — but it is exactly why a typed catch needs the gateway instead.
        assertThatThrownBy(() -> ordersIn.send(message))
                .isInstanceOf(MessagingException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void theFlowRunsOnTheCallingThread() {
        // A queue or executor channel anywhere in the flow would break this, and with it the
        // meaning of "the handler returned" for the starter.
        when(producer.send(eq("flow-invoices"), any(), anyString()))
                .thenAnswer(inv -> {
                    TestConfig.FLOW_THREAD.set(Thread.currentThread().getName());
                    return new ResilientProducer.SendResult(true, "primary", "inv-o-1");
                });

        ordersIn.send(order(100));

        assertThat(TestConfig.FLOW_THREAD.get()).isEqualTo(Thread.currentThread().getName());
    }

    @Test
    void billingGatewayReturnsTheInvoiceItsFlowProduced() {
        BillingGateway gateway = ctx.getBean(BillingGateway.class);

        Invoice invoice = gateway.bill(order(250));

        assertThat(invoice.orderId()).isEqualTo("o-1");
        assertThat(invoice.amountDue()).isEqualTo(250);
    }

    @Test
    void gatewayReportsTheOriginalExceptionWhileSendWrapsIt() {
        // The reason BillingBatchProcessor calls a gateway instead of a channel: only the
        // unwrapped exception matches catch (UnprocessableOrderException), which is what
        // turns a poison record into a discard rather than an endless retry.
        MessageChannel boomIn = ctx.getBean("boomIn", MessageChannel.class);
        ProbeGateway probe = ctx.getBean(ProbeGateway.class);
        Message<OrderEvent> message = order(100);

        assertThatThrownBy(() -> boomIn.send(message))
                .isInstanceOf(MessagingException.class)
                .hasRootCauseInstanceOf(UnprocessableOrderException.class);

        assertThatThrownBy(() -> probe.explode(message))
                .isInstanceOf(UnprocessableOrderException.class);
    }

    /** The real {@link OrderFlowConfig} is imported, so the test drives the shipped flows. */
    @Configuration
    @EnableIntegration
    @IntegrationComponentScan(basePackages = "dev.semeshin.kafkadr")
    @Import(OrderFlowConfig.class)
    static class TestConfig {

        static final AtomicReference<ResilientProducer> PRODUCER = new AtomicReference<>();
        static final AtomicReference<String> FLOW_THREAD = new AtomicReference<>();

        @Bean ResilientProducer resilientProducer() { return PRODUCER.get(); }
        @Bean FlowMetrics flowMetrics() { return new FlowMetrics(); }

        @Bean MessageChannel boomIn() { return new DirectChannel(); }

        @Bean
        IntegrationFlow boomFlow() {
            return IntegrationFlow.from("boomIn")
                    .handle(m -> { throw new UnprocessableOrderException("poison"); })
                    .get();
        }
    }

    @org.springframework.integration.annotation.MessagingGateway
    public interface ProbeGateway {
        @org.springframework.integration.annotation.Gateway(requestChannel = "boomIn", replyTimeout = 1_000)
        void explode(Message<OrderEvent> message);
    }
}
