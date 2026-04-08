package com.example.kafkadr.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Central place for all topic message processing methods.
 *
 * Each method must accept a single Message<String> parameter.
 * Map methods to topics via application.yml:
 *
 * <pre>
 * kafka-dr:
 *   consumers:
 *     - topic: demo-events
 *       handler: processDemoEvent
 *     - topic: order-events
 *       handler: processOrder
 * </pre>
 */
@Component
public class MessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    public void processDemoEvent(Message<String> message) {
        log.info("[demo-events] Processing demo event: {}", message.getPayload());
    }

    public void processOrder(Message<String> message) {
        log.info("[order-events] Processing order: {}", message.getPayload());
        // orderService.process(message.getPayload());
    }
}
