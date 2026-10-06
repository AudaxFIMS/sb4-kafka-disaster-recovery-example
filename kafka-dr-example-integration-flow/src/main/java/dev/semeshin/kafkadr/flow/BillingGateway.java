package dev.semeshin.kafkadr.flow;

import dev.semeshin.kafkadr.model.Invoice;
import dev.semeshin.kafkadr.model.OrderEvent;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.messaging.Message;

/**
 * Request/reply entry into {@code billingFlow}, used by the batch handler.
 *
 * <p>Why a gateway rather than {@code channel.send()}: a gateway <b>unwraps</b> exceptions,
 * so {@code catch (UnprocessableOrderException e)} in the caller matches. A plain
 * {@code send()} wraps the failure in a {@code MessagingException} — {@code
 * MessageDeliveryException} from the channel's immediate subscriber, {@code
 * MessageHandlingException} from further down the flow — the typed catch misses, and a poison
 * record ends up retried forever instead of discarded.
 *
 * <p>Why this flow has no filter: a reply is expected, and a filtered-out message produces no
 * reply at all — the call would block until {@code replyTimeout} and then return {@code null}.
 * Records that must not be billed are rejected by the handler <i>before</i> the gateway.
 */
@MessagingGateway
public interface BillingGateway {

    @Gateway(requestChannel = "billingIn", replyTimeout = 5_000)
    Invoice bill(Message<OrderEvent> order);
}
