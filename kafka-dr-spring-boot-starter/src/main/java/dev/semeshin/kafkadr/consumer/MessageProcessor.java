package dev.semeshin.kafkadr.consumer;

/**
 * Marker interface for message handler beans.
 * Implement this interface and annotate with @Component to register handler methods.
 * Handler methods must accept a single Message<?> parameter and be referenced
 * by name in kafka-dr.consumers[].handler configuration.
 *
 * <pre>
 * {@code
 * @Component
 * public class OrderMessageProcessor implements MessageProcessor {
 *
 *     public void processOrder(Message<OrderEvent> message) {
 *         OrderEvent order = message.getPayload();
 *         // business logic
 *     }
 * }
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * kafka-dr:
 *   consumers:
 *     - topic: order-events
 *       handler: processOrder
 *       content-type: json
 * }
 * </pre>
 */
public interface MessageProcessor {
}
