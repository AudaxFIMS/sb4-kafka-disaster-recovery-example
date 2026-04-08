package com.example.kafkadr.consumer;

import com.example.kafkadr.config.KafkaClusterProperties;
import com.example.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Builds a topic -> handler mapping by resolving method names from configuration
 * against the MessageProcessor bean.
 */
@Component
public class MessageHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(MessageHandlerRegistry.class);

    private final Map<String, Consumer<Message<String>>> handlers = new HashMap<>();

    private final Consumer<Message<String>> defaultHandler =
            msg -> log.info("[unhandled] Received: {}", msg.getPayload());

    public MessageHandlerRegistry(MessageProcessor processor,
                                  KafkaClusterProperties properties) {
        for (ConsumerConfig consumer : properties.getConsumers()) {
            String topic = consumer.getTopic();
            String handlerName = consumer.getHandler();

            if (handlerName == null || handlerName.isBlank()) {
                log.info("No handler configured for topic '{}', using default", topic);
                handlers.put(topic, defaultHandler);
                continue;
            }

            try {
                Method method = processor.getClass().getMethod(handlerName, Message.class);
                handlers.put(topic, msg -> {
                    try {
                        method.invoke(processor, msg);
                    } catch (Exception e) {
                        log.error("[{}] Handler '{}' failed: {}", topic, handlerName, e.getMessage(), e);
                    }
                });
                log.info("Mapped topic '{}' -> MessageProcessor.{}()", topic, handlerName);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(
                        "Handler method '%s' not found in MessageProcessor for topic '%s'. "
                                .formatted(handlerName, topic)
                        + "Expected: public void %s(Message<String> message)".formatted(handlerName));
            }
        }
    }

    public Consumer<Message<String>> getHandler(String topic) {
        return handlers.getOrDefault(topic, defaultHandler);
    }
}
