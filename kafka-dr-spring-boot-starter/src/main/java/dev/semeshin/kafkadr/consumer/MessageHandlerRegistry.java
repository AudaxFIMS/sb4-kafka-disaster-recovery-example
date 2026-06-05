package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class MessageHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(MessageHandlerRegistry.class);

    private final Map<String, Consumer<Message<?>>> handlers = new HashMap<>();
    private final ObjectMapper objectMapper;

    private final Consumer<Message<?>> defaultHandler =
            msg -> log.info("[unhandled] Received message, key={}", msg.getHeaders().get(KafkaHeaders.RECEIVED_KEY));

    public MessageHandlerRegistry(List<MessageProcessor> processors,
                                  KafkaClusterProperties properties) {
        this.objectMapper = new ObjectMapper();

        for (ConsumerConfig consumer : properties.getConsumers().values()) {
            String consumerName = consumer.getName();
            String handlerName = consumer.getHandler();

            if (handlerName == null || handlerName.isBlank()) {
                log.info("No handler configured for consumer '{}', using default", consumerName);
                handlers.put(consumerName, defaultHandler);
                continue;
            }

            HandlerRef ref = findHandler(processors, handlerName);
            Class<?> payloadType = extractPayloadType(ref.method());
            String contentType = consumer.getContentType();

            handlers.put(consumerName, msg -> {
                try {
                    Message<?> converted = convertPayload(msg, payloadType, contentType);
                    ref.method().invoke(ref.bean(), converted);
                } catch (Exception e) {
                    log.error("[{}] Handler '{}' failed: {}", consumerName, handlerName, e.getMessage(), e);
                }
            });
            log.info("Mapped consumer '{}' (topic={}) -> {}.{}(Message<{}>) [content-type={}]",
                    consumerName, consumer.getTopic(), ref.bean().getClass().getSimpleName(), handlerName,
                    payloadType.getSimpleName(), contentType);
        }
    }

    private record HandlerRef(Object bean, Method method) {}

    private Message<?> convertPayload(Message<?> original, Class<?> targetType, String contentType) {
        Object payload = original.getPayload();

        // Already the right type — no conversion needed (Avro, native, or matching type)
        if (targetType.isInstance(payload)) {
            return original;
        }

        // "native" — Kafka deserializer should have produced the target type already.
        // If it didn't match above, log a warning but pass through.
        if ("native".equalsIgnoreCase(contentType)) {
            log.warn("Native content-type but payload {} is not assignable to {}. "
                            + "Check Kafka deserializer configuration.",
                    payload.getClass().getSimpleName(), targetType.getSimpleName());
            return original;
        }

        // "bytes" — pass raw byte[], no conversion
        if ("bytes".equalsIgnoreCase(contentType)) {
            return original;
        }

        // Convert from byte[] or String based on content-type
        if (payload instanceof byte[] bytes) {
            Object converted = convertBytes(bytes, targetType, contentType);
            return MessageBuilder.withPayload(converted)
                    .copyHeaders(original.getHeaders())
                    .build();
        }

        if (payload instanceof String str && !targetType.equals(String.class)) {
            Object converted = convertString(str, targetType, contentType);
            return MessageBuilder.withPayload(converted)
                    .copyHeaders(original.getHeaders())
                    .build();
        }

        return original;
    }

    private Object convertBytes(byte[] bytes, Class<?> targetType, String contentType) {
        if (targetType.equals(byte[].class)) {
            return bytes;
        }
        if ("string".equalsIgnoreCase(contentType) || targetType.equals(String.class)) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        // "json" (default) — deserialize via Jackson
        try {
            return objectMapper.readValue(bytes, targetType);
        } catch (Exception e) {
            log.warn("JSON deserialization to {} failed, falling back to String: {}",
                    targetType.getSimpleName(), e.getMessage());
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private Object convertString(String str, Class<?> targetType, String contentType) {
        if (targetType.equals(byte[].class)) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        if ("string".equalsIgnoreCase(contentType)) {
            return str;
        }
        // "json" — parse from JSON string
        try {
            return objectMapper.readValue(str, targetType);
        } catch (Exception e) {
            log.warn("JSON deserialization to {} failed: {}", targetType.getSimpleName(), e.getMessage());
            return str;
        }
    }

    private Class<?> extractPayloadType(Method method) {
        Type paramType = method.getGenericParameterTypes()[0];
        if (paramType instanceof ParameterizedType pt) {
            Type[] typeArgs = pt.getActualTypeArguments();
            if (typeArgs.length == 1 && typeArgs[0] instanceof Class<?> clazz) {
                return clazz;
            }
        }
        return Object.class;
    }

    private HandlerRef findHandler(List<MessageProcessor> processors, String methodName) {
        for (MessageProcessor processor : processors) {
            for (Method method : processor.getClass().getMethods()) {
                if (method.getName().equals(methodName)
                        && method.getParameterCount() == 1
                        && Message.class.isAssignableFrom(method.getParameterTypes()[0])) {
                    return new HandlerRef(processor, method);
                }
            }
        }
        String beanNames = processors.stream()
                .map(p -> p.getClass().getSimpleName())
                .reduce((a, b) -> a + ", " + b)
                .orElse("none");
        throw new IllegalStateException(
                "Handler method '%s(Message<?> message)' not found in any MessageProcessor bean. Available beans: [%s]"
                        .formatted(methodName, beanNames));
    }

    /**
     * @param consumerName the logical consumer name (from kafka-dr.consumers map key)
     */
    public Consumer<Message<?>> getHandler(String consumerName) {
        return handlers.getOrDefault(consumerName, defaultHandler);
    }
}
