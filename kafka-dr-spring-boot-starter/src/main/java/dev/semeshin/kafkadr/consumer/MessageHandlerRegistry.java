package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class MessageHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(MessageHandlerRegistry.class);

    /**
     * The parameter shapes a handler method may declare. The shape decides how the
     * starter drives the handler and which consumer bean is compatible with it.
     */
    public enum Shape {
        /** {@code void h(Message<T>)} — one record at a time. */
        SINGLE,
        /** {@code void h(List<Message<T>>)} — the deduplicated batch, split mode. */
        LIST_OF_MESSAGE,
        /** {@code void h(Message<List<T>>)} — the raw batch envelope, standard mode. */
        MESSAGE_OF_LIST,
        /** {@code void h(List<T>)} — payloads only, standard mode. */
        LIST_OF_PAYLOAD,
        /** {@code BatchOutcome h(List<Message<T>>)} — the batch, with per-record verdicts. */
        BATCH_OUTCOME;

        boolean isBatch() {
            return this != SINGLE;
        }

        boolean deliversEnvelope() {
            return this == MESSAGE_OF_LIST || this == LIST_OF_PAYLOAD;
        }
    }

    private record HandlerRef(Object bean, Method method, Shape shape, Class<?> elementType) {}

    private final Map<String, HandlerRef> refs = new HashMap<>();
    private final Map<String, ConsumerConfig> consumers = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Diagnostic logging switch — see {@code kafka-dr.debug.enable}. */
    private final boolean debugEnabled;

    private final Consumer<Message<?>> defaultHandler = msg -> {
        if (BatchMessages.isBatch(msg)) {
            log.info("[unhandled] Received batch of {}", ((List<?>) msg.getPayload()).size());
        } else {
            log.info("[unhandled] Received message, key={}", msg.getHeaders().get(KafkaHeaders.RECEIVED_KEY));
        }
    };

    public MessageHandlerRegistry(List<MessageProcessor> processors,
                                  KafkaClusterProperties properties) {
        this.debugEnabled = properties.getDebug().isEnable();
        for (ConsumerConfig consumer : properties.getConsumers().values()) {
            String consumerName = consumer.getName();
            consumers.put(consumerName, consumer);

            String handlerName = consumer.getHandler();
            if (handlerName == null || handlerName.isBlank()) {
                log.info("No handler configured for consumer '{}', using default", consumerName);
                continue;
            }

            HandlerRef ref = findHandler(processors, handlerName);
            validate(consumer, ref);
            refs.put(consumerName, ref);

            log.info("Mapped consumer '{}' (topic={}) -> {}.{}({}) [shape={}, element={}, content-type={}]",
                    consumerName, consumer.getTopic(), ref.bean().getClass().getSimpleName(), handlerName,
                    ref.method().getGenericParameterTypes()[0].getTypeName(), ref.shape(),
                    ref.elementType().getSimpleName(), consumer.getContentType());
        }
    }

    // --- lookup ---------------------------------------------------------------

    /** Shape declared by the consumer's handler, or SINGLE when no handler is configured. */
    public Shape shapeOf(String consumerName) {
        HandlerRef ref = refs.get(consumerName);
        return ref == null ? Shape.SINGLE : ref.shape();
    }

    /**
     * Per-record invoker used by the non-batch consumer. Split mode does not come through
     * here — it drives the same handler through {@link #getBatchHandler}, which reports
     * failures through {@link BatchHandler.Result} instead of throwing.
     *
     * <p>A failing handler propagates, exactly as on the batch paths. Swallowing it here
     * would mark the record processed in the {@code IdempotencyStore}, advance the
     * seek-by-timestamp watermark and commit the offset for a record that was never
     * handled — and the redelivery Kafka performs would then be dropped as a duplicate.
     * Bound the redelivery with the binding's {@code max-attempts} / {@code enable-dlq},
     * or catch inside the handler where a failure really is not worth a retry.
     */
    public Consumer<Message<?>> getHandler(String consumerName) {
        HandlerRef ref = refs.get(consumerName);
        if (ref == null) {
            return defaultHandler;
        }
        requireShape(consumerName, ref, false);
        String contentType = contentTypeOf(consumerName);
        return msg -> {
            try {
                invoke(ref, convertPayload(msg, ref.elementType(), contentType));
            } catch (RuntimeException e) {
                // Logged with the consumer name the container's error handler does not know,
                // then propagated so the caller can roll back and let Kafka redeliver.
                if (debugEnabled) {
                    log.error("[{}] Handler '{}' failed, propagating: {}",
                            consumerName, ref.method().getName(), e.getClass().getSimpleName(), e);
                } else {
                    log.error("[{}] Handler '{}' failed, propagating: {} - {}",
                            consumerName, ref.method().getName(), e.getClass().getSimpleName(), e.getMessage());
                }
                throw e;
            }
        };
    }

    /**
     * Drives a deduplicated batch in split mode, honouring the consumer's error policy.
     * Exceptions are reported through the result rather than swallowed, so the caller can
     * commit the prefix, roll back the tail and let the container redeliver.
     */
    public BatchHandler getBatchHandler(String consumerName) {
        HandlerRef ref = refs.get(consumerName);
        BatchConfig.ErrorPolicy policy = batchOf(consumerName).getErrorPolicy();

        if (ref == null) {
            return messages -> {
                messages.forEach(defaultHandler);
                return BatchHandler.Result.COMPLETE;
            };
        }
        requireShape(consumerName, ref, false);

        return switch (ref.shape()) {
            case BATCH_OUTCOME -> batchOutcomeHandler(consumerName, ref);
            case LIST_OF_MESSAGE -> listOfMessageHandler(consumerName, ref);
            default -> perRecordHandler(consumerName, ref, policy);
        };
    }

    /**
     * Delivers the raw batch envelope in standard mode. Nothing is caught here — error
     * handling belongs entirely to the handler, which is what makes this mode behave like
     * plain Spring Cloud Stream.
     */
    public Consumer<Message<?>> getEnvelopeHandler(String consumerName) {
        HandlerRef ref = refs.get(consumerName);
        if (ref == null) {
            return defaultHandler;
        }
        requireShape(consumerName, ref, true);
        String contentType = contentTypeOf(consumerName);

        return envelope -> {
            List<Object> converted = convertElements(envelope, ref.elementType(), contentType);
            if (ref.shape() == Shape.LIST_OF_PAYLOAD) {
                invoke(ref, converted);
            } else {
                invoke(ref, MessageBuilder.withPayload(converted)
                        .copyHeaders(envelope.getHeaders())
                        .build());
            }
        };
    }

    // --- batch drivers --------------------------------------------------------

    private BatchHandler perRecordHandler(String consumerName, HandlerRef ref,
                                          BatchConfig.ErrorPolicy policy) {
        String contentType = contentTypeOf(consumerName);
        return messages -> {
            List<Message<?>> skipped = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                Message<?> record = messages.get(i);
                try {
                    invoke(ref, convertPayloadStrictly(record, i, ref.elementType(), contentType));
                } catch (RuntimeException e) {
                    if (policy == BatchConfig.ErrorPolicy.SKIP_FAILED) {
                        log.error("[{}] Record {} failed, skipping (error-policy=skip-failed): {}",
                                consumerName, i, e.getMessage(), e);
                        skipped.add(record);
                        continue;
                    }
                    return BatchHandler.Result.stoppedAt(i, messages, e);
                }
            }
            return BatchHandler.Result.skipping(skipped);
        };
    }

    private BatchHandler listOfMessageHandler(String consumerName, HandlerRef ref) {
        String contentType = contentTypeOf(consumerName);
        return messages -> {
            List<Message<?>> converted = new ArrayList<>(messages.size());
            try {
                for (int i = 0; i < messages.size(); i++) {
                    converted.add(convertPayloadStrictly(messages.get(i), i, ref.elementType(), contentType));
                }
                invoke(ref, converted);
            } catch (BatchConversionException e) {
                return BatchHandler.Result.stoppedAt(e.getIndex(), messages, e);
            } catch (RuntimeException e) {
                // One call for the whole list: which record failed is unknowable, so the
                // batch is retried from its first record. BatchOutcome (phase 3) is the
                // way to report per-record outcomes back to the starter.
                log.error("[{}] Batch handler '{}' failed: {}",
                        consumerName, ref.method().getName(), e.getMessage(), e);
                return BatchHandler.Result.stoppedAt(0, messages, e);
            }
            return BatchHandler.Result.COMPLETE;
        };
    }

    /**
     * Lets the handler report a verdict per record. Outcomes need not be contiguous, so
     * the rollback set is taken from the outcome rather than derived from the stop index:
     * a record marked done after a retried one stays marked and is deduplicated on
     * redelivery instead of being processed twice.
     */
    private BatchHandler batchOutcomeHandler(String consumerName, HandlerRef ref) {
        String contentType = contentTypeOf(consumerName);
        return messages -> {
            List<Message<?>> converted = new ArrayList<>(messages.size());
            try {
                for (int i = 0; i < messages.size(); i++) {
                    converted.add(convertPayloadStrictly(messages.get(i), i, ref.elementType(), contentType));
                }
            } catch (BatchConversionException e) {
                return BatchHandler.Result.stoppedAt(e.getIndex(), messages, e);
            }

            BatchOutcome outcome;
            try {
                outcome = (BatchOutcome) invokeWithResult(ref, converted);
            } catch (RuntimeException e) {
                log.error("[{}] Batch handler '{}' failed before reporting outcomes: {}",
                        consumerName, ref.method().getName(), e.getMessage(), e);
                return BatchHandler.Result.stoppedAt(0, messages, e);
            }
            if (outcome == null) {
                return BatchHandler.Result.stoppedAt(0, messages,
                        new IllegalStateException("Handler '" + ref.method().getName() + "' returned null"));
            }

            int unmarked = outcome.unmarkedCount();
            if (unmarked > 0) {
                log.warn("[{}] Handler '{}' left {} of {} records unmarked; they are treated as retry",
                        consumerName, ref.method().getName(), unmarked, outcome.size());
            }

            int firstRetry = outcome.firstRetryIndex();
            // Marks are released only for retried records; done and discard both stay,
            // one because it succeeded and the other because repeating it would fail again.
            List<Message<?>> rollback = rebase(outcome.retried(), converted, messages);
            if (firstRetry < 0) {
                return BatchHandler.Result.COMPLETE;
            }
            Throwable cause = outcome.firstRetryCause();
            RuntimeException failure = cause instanceof RuntimeException re
                    ? re
                    : new IllegalStateException("Record " + firstRetry + " requires redelivery", cause);
            return new BatchHandler.Result(firstRetry, rollback, failure);
        };
    }

    /**
     * Maps converted messages back to the originals the caller holds. Conversion may have
     * rebuilt a message, and the caller identifies records by identity to find their
     * offset in the batch.
     */
    private static List<Message<?>> rebase(List<Message<?>> subset,
                                           List<Message<?>> converted,
                                           List<Message<?>> originals) {
        List<Message<?>> rebased = new ArrayList<>(subset.size());
        for (Message<?> message : subset) {
            int index = indexOfIdentity(converted, message);
            rebased.add(index < 0 ? message : originals.get(index));
        }
        return rebased;
    }

    private static int indexOfIdentity(List<Message<?>> list, Message<?> target) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == target) {
                return i;
            }
        }
        return -1;
    }

    // --- conversion -----------------------------------------------------------

    /** Elementwise conversion of a batch envelope payload. */
    private List<Object> convertElements(Message<?> envelope, Class<?> elementType, String contentType) {
        if (!(envelope.getPayload() instanceof List<?> raw)) {
            return List.of(convertValue(envelope.getPayload(), 0, elementType, contentType));
        }
        List<Object> converted = new ArrayList<>(raw.size());
        for (int i = 0; i < raw.size(); i++) {
            converted.add(convertValue(raw.get(i), i, elementType, contentType));
        }
        return converted;
    }

    private Message<?> convertPayloadStrictly(Message<?> original, int index,
                                              Class<?> targetType, String contentType) {
        Object converted = convertValue(original.getPayload(), index, targetType, contentType);
        return converted == original.getPayload()
                ? original
                : MessageBuilder.withPayload(converted).copyHeaders(original.getHeaders()).build();
    }

    /**
     * Converts one value, failing loudly instead of substituting a fallback.
     *
     * <p>A null value is a record the deserializer could not read. In record mode the
     * container filters those out before the listener runs; in batch mode it does not
     * (checkDeser is only called on the record path), so they arrive here.
     */
    private Object convertValue(Object payload, int index, Class<?> targetType, String contentType) {
        if (payload == null || payload instanceof KafkaNull) {
            throw new BatchConversionException(index,
                    ("Record %d has no value — deserialization failed upstream (check the "
                            + "DeserializationException header). Unlike record mode, batch mode does not "
                            + "filter unreadable records out before the listener runs, so they arrive here.")
                            .formatted(index), null);
        }
        if (targetType.isInstance(payload)) {
            return payload;
        }
        if ("native".equalsIgnoreCase(contentType) || "bytes".equalsIgnoreCase(contentType)) {
            return payload;
        }
        try {
            if (payload instanceof byte[] bytes) {
                return convertBytes(bytes, targetType, contentType);
            }
            if (payload instanceof String str) {
                return convertString(str, targetType, contentType);
            }
        } catch (Exception e) {
            throw new BatchConversionException(index,
                    "Record %d could not be converted to %s".formatted(index, targetType.getName()), e);
        }
        throw new BatchConversionException(index,
                ("Record %d has payload type %s, which cannot be converted to %s with content-type=%s. "
                        + "Passing it through would only surface as a ClassCastException inside the handler.")
                        .formatted(index, payload.getClass().getName(), targetType.getName(), contentType), null);
    }

    /**
     * Record-path conversion, unchanged: a failed JSON parse still falls back to the raw
     * string. Batch paths use {@link #convertValue} instead, which throws — putting a
     * String into a {@code List<T>} would only surface as a ClassCastException later.
     */
    private Message<?> convertPayload(Message<?> original, Class<?> targetType, String contentType) {
        Object payload = original.getPayload();

        if (targetType.isInstance(payload)) {
            return original;
        }
        if ("native".equalsIgnoreCase(contentType)) {
            log.warn("Native content-type but payload {} is not assignable to {}. "
                            + "Check Kafka deserializer configuration.",
                    payload.getClass().getSimpleName(), targetType.getSimpleName());
            return original;
        }
        if ("bytes".equalsIgnoreCase(contentType)) {
            return original;
        }
        if (payload instanceof byte[] bytes) {
            Object converted = convertBytesLeniently(bytes, targetType, contentType);
            return MessageBuilder.withPayload(converted).copyHeaders(original.getHeaders()).build();
        }
        if (payload instanceof String str && !targetType.equals(String.class)) {
            Object converted = convertStringLeniently(str, targetType, contentType);
            return MessageBuilder.withPayload(converted).copyHeaders(original.getHeaders()).build();
        }
        return original;
    }

    private Object convertBytes(byte[] bytes, Class<?> targetType, String contentType) throws Exception {
        if (targetType.equals(byte[].class)) {
            return bytes;
        }
        if ("string".equalsIgnoreCase(contentType) || targetType.equals(String.class)) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return objectMapper.readValue(bytes, targetType);
    }

    private Object convertString(String str, Class<?> targetType, String contentType) throws Exception {
        if (targetType.equals(byte[].class)) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        if ("string".equalsIgnoreCase(contentType)) {
            return str;
        }
        return objectMapper.readValue(str, targetType);
    }

    private Object convertBytesLeniently(byte[] bytes, Class<?> targetType, String contentType) {
        try {
            return convertBytes(bytes, targetType, contentType);
        } catch (Exception e) {
            log.warn("JSON deserialization to {} failed, falling back to String: {}",
                    targetType.getSimpleName(), e.getMessage());
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private Object convertStringLeniently(String str, Class<?> targetType, String contentType) {
        try {
            return convertString(str, targetType, contentType);
        } catch (Exception e) {
            log.warn("JSON deserialization to {} failed: {}", targetType.getSimpleName(), e.getMessage());
            return str;
        }
    }

    // --- reflection -----------------------------------------------------------

    private static void invoke(HandlerRef ref, Object argument) {
        invokeWithResult(ref, argument);
    }

    private static Object invokeWithResult(HandlerRef ref, Object argument) {
        try {
            return ref.method().invoke(ref.bean(), argument);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Handler '" + ref.method().getName() + "' failed", cause);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Handler '" + ref.method().getName() + "' is not accessible", e);
        }
    }

    private HandlerRef findHandler(List<MessageProcessor> processors, String methodName) {
        for (MessageProcessor processor : processors) {
            for (Method method : processor.getClass().getMethods()) {
                if (!method.getName().equals(methodName) || method.getParameterCount() != 1) {
                    continue;
                }
                Class<?> paramType = method.getParameterTypes()[0];
                if (!Message.class.isAssignableFrom(paramType) && !List.class.isAssignableFrom(paramType)) {
                    continue;
                }
                return describe(processor, method);
            }
        }
        String beanNames = processors.stream()
                .map(p -> p.getClass().getSimpleName())
                .reduce((a, b) -> a + ", " + b)
                .orElse("none");
        throw new IllegalStateException(
                ("Handler method '%s' not found in any MessageProcessor bean. Expected one parameter of type "
                        + "Message<T>, List<Message<T>>, Message<List<T>> or List<T>. Available beans: [%s]")
                        .formatted(methodName, beanNames));
    }

    /** Derives the shape and the element type from the single parameter's generic type. */
    private static HandlerRef describe(Object bean, Method method) {
        Type param = method.getGenericParameterTypes()[0];
        Type inner = typeArgument(param);

        if (Message.class.isAssignableFrom(method.getParameterTypes()[0])) {
            if (rawType(inner) != null && List.class.isAssignableFrom(rawType(inner))) {
                return new HandlerRef(bean, method, Shape.MESSAGE_OF_LIST, elementOf(typeArgument(inner)));
            }
            return new HandlerRef(bean, method, Shape.SINGLE, elementOf(inner));
        }
        if (rawType(inner) != null && Message.class.isAssignableFrom(rawType(inner))) {
            Shape shape = BatchOutcome.class.isAssignableFrom(method.getReturnType())
                    ? Shape.BATCH_OUTCOME
                    : Shape.LIST_OF_MESSAGE;
            return new HandlerRef(bean, method, shape, elementOf(typeArgument(inner)));
        }
        return new HandlerRef(bean, method, Shape.LIST_OF_PAYLOAD, elementOf(inner));
    }

    private static Type typeArgument(Type type) {
        if (type instanceof ParameterizedType pt && pt.getActualTypeArguments().length == 1) {
            return pt.getActualTypeArguments()[0];
        }
        return null;
    }

    private static Class<?> rawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return null;
    }

    private static Class<?> elementOf(Type type) {
        Class<?> raw = rawType(type);
        return raw == null ? Object.class : raw;
    }

    // --- validation -----------------------------------------------------------

    private static void validate(ConsumerConfig consumer, HandlerRef ref) {
        String name = consumer.getName();
        BatchConfig batch = consumer.getBatch();

        if (!batch.isEnabled()) {
            if (ref.shape().isBatch()) {
                throw new IllegalStateException(
                        ("Consumer '%s' has a batch handler (%s) but batching is off. Set "
                                + "kafka-dr.consumers.%s.batch.enabled=true.")
                                .formatted(name, ref.shape(), name));
            }
            return;
        }

        boolean standard = batch.getMode() == BatchConfig.Mode.STANDARD;
        if (standard != ref.shape().deliversEnvelope()) {
            throw new IllegalStateException(
                    ("Consumer '%s' uses batch.mode=%s but its handler declares %s. Use "
                            + "Message<List<T>> or List<T> with mode=standard, and Message<T> or "
                            + "List<Message<T>> with mode=split.")
                            .formatted(name, batch.getMode().name().toLowerCase(), ref.shape()));
        }

        if ((ref.shape() == Shape.LIST_OF_MESSAGE || ref.shape() == Shape.BATCH_OUTCOME)
                && batch.getErrorPolicy() == BatchConfig.ErrorPolicy.SKIP_FAILED) {
            throw new IllegalStateException(
                    ("Consumer '%s' combines error-policy=skip-failed with a %s handler. The handler is "
                            + "invoked once for the whole list, so an individual record cannot be skipped. "
                            + "Use a Message<T> handler, error-policy=fail-batch, or a BatchOutcome handler "
                            + "that reports per-record verdicts.")
                            .formatted(name, ref.shape()));
        }
    }

    private void requireShape(String consumerName, HandlerRef ref, boolean envelope) {
        if (ref.shape().deliversEnvelope() != envelope) {
            throw new IllegalStateException(
                    "Consumer '%s' handler shape %s cannot be driven this way".formatted(consumerName, ref.shape()));
        }
    }

    private String contentTypeOf(String consumerName) {
        ConsumerConfig consumer = consumers.get(consumerName);
        return consumer == null ? "json" : consumer.getContentType();
    }

    private BatchConfig batchOf(String consumerName) {
        ConsumerConfig consumer = consumers.get(consumerName);
        return consumer == null ? new BatchConfig() : consumer.getBatch();
    }
}
