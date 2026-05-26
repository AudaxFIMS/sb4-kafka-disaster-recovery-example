package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageHandlerRegistryTest {

    @Test
    void wiresTopicHandlerByMethodName() {
        AtomicReference<String> captured = new AtomicReference<>();
        TestProcessor processor = new TestProcessor(captured);

        KafkaClusterProperties props = propsFor("orders", "processOrder", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        Consumer<Message<?>> handler = registry.getHandler("orders");

        handler.accept(MessageBuilder.withPayload("{\"id\":\"o-1\"}".getBytes(StandardCharsets.UTF_8)).build());

        assertThat(captured.get()).isEqualTo("o-1");
    }

    @Test
    void stringContentTypeConvertsBytesToString() {
        AtomicReference<Object> captured = new AtomicReference<>();
        StringProcessor processor = new StringProcessor(captured);
        KafkaClusterProperties props = propsFor("events", "handleEvent", "string");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("events").accept(
                MessageBuilder.withPayload("hello".getBytes(StandardCharsets.UTF_8)).build());

        assertThat(captured.get()).isEqualTo("hello");
    }

    @Test
    void bytesContentTypePassesPayloadThrough() {
        AtomicReference<Object> captured = new AtomicReference<>();
        BytesProcessor processor = new BytesProcessor(captured);
        KafkaClusterProperties props = propsFor("raw", "handleRaw", "bytes");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        byte[] data = {1, 2, 3};
        registry.getHandler("raw").accept(MessageBuilder.withPayload(data).build());

        assertThat(captured.get()).isEqualTo(data);
    }

    @Test
    void unknownTopicYieldsDefaultHandlerNotThrowing() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(), props);

        registry.getHandler("nonexistent").accept(MessageBuilder.withPayload("x").build());
    }

    @Test
    void missingHandlerMethodThrowsAtConstruction() {
        KafkaClusterProperties props = propsFor("orders", "noSuchMethod", "json");

        assertThatThrownBy(() -> new MessageHandlerRegistry(List.of(new TestProcessor(new AtomicReference<>())), props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("noSuchMethod");
    }

    @Test
    void blankHandlerUsesDefault() {
        KafkaClusterProperties props = propsFor("orders", "", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(), props);

        registry.getHandler("orders").accept(MessageBuilder.withPayload("x").build());
    }

    @Test
    void payloadAlreadyMatchingTargetTypeBypassesConversion() {
        AtomicReference<Object> captured = new AtomicReference<>();
        StringProcessor processor = new StringProcessor(captured);
        KafkaClusterProperties props = propsFor("events", "handleEvent", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("events").accept(MessageBuilder.withPayload("already-string").build());

        assertThat(captured.get()).isEqualTo("already-string");
    }

    @Test
    void nativeContentTypeWithMatchingPayloadIsPassedThrough() {
        AtomicReference<Object> captured = new AtomicReference<>();
        StringProcessor processor = new StringProcessor(captured);
        KafkaClusterProperties props = propsFor("events", "handleEvent", "native");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("events").accept(MessageBuilder.withPayload("native-payload").build());

        assertThat(captured.get()).isEqualTo("native-payload");
    }

    @Test
    void nativeContentTypeWithMismatchedPayloadLogsWarningAndPassesThrough() {
        AtomicReference<Object> captured = new AtomicReference<>();
        BytesProcessor processor = new BytesProcessor(captured);
        KafkaClusterProperties props = propsFor("raw", "handleRaw", "native");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("raw").accept(MessageBuilder.withPayload("not-bytes").build());
    }

    @Test
    void jsonStringPayloadIsDeserializedToTarget() {
        AtomicReference<String> captured = new AtomicReference<>();
        TestProcessor processor = new TestProcessor(captured);
        KafkaClusterProperties props = propsFor("orders", "processOrder", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("orders").accept(MessageBuilder.withPayload("{\"id\":\"from-string\"}").build());

        assertThat(captured.get()).isEqualTo("from-string");
    }

    @Test
    void stringContentTypeWithStringPayloadTargetingByteArrayConvertsToBytes() {
        AtomicReference<Object> captured = new AtomicReference<>();
        BytesProcessor processor = new BytesProcessor(captured);
        KafkaClusterProperties props = propsFor("raw", "handleRaw", "string");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("raw").accept(MessageBuilder.withPayload("text").build());

        assertThat(captured.get()).isInstanceOf(byte[].class);
        assertThat(new String((byte[]) captured.get(), StandardCharsets.UTF_8)).isEqualTo("text");
    }

    @Test
    void invalidJsonPayloadFallsBackToStringWithoutCrashing() {
        TestProcessor processor = new TestProcessor(new AtomicReference<>());
        KafkaClusterProperties props = propsFor("orders", "processOrder", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);

        registry.getHandler("orders").accept(
                MessageBuilder.withPayload("not-json".getBytes(StandardCharsets.UTF_8)).build());
    }

    @Test
    void bytesContentTypeWithMismatchedPayloadPassesThrough() {
        AtomicReference<Object> captured = new AtomicReference<>();
        BytesProcessor processor = new BytesProcessor(captured);
        KafkaClusterProperties props = propsFor("raw", "handleRaw", "bytes");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("raw").accept(MessageBuilder.withPayload("not-bytes-payload").build());

        assertThat(captured.get()).isEqualTo("not-bytes-payload");
    }

    @Test
    void unconvertibleNonStringNonBytesPayloadIsReturnedAsIs() {
        StringProcessor processor = new StringProcessor(new AtomicReference<>());
        KafkaClusterProperties props = propsFor("events", "handleEvent", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("events").accept(MessageBuilder.withPayload(Integer.valueOf(42)).build());
    }

    @Test
    void convertStringForStringContentTypeWithPojoTargetReturnsRawString() {
        AtomicReference<String> captured = new AtomicReference<>();
        TestProcessor processor = new TestProcessor(captured);
        KafkaClusterProperties props = propsFor("orders", "processOrder", "string");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("orders").accept(MessageBuilder.withPayload("{\"id\":\"o-1\"}").build());
    }

    @Test
    void convertStringJsonParseFailureFallsBackWithoutCrash() {
        TestProcessor processor = new TestProcessor(new AtomicReference<>());
        KafkaClusterProperties props = propsFor("orders", "processOrder", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        registry.getHandler("orders").accept(MessageBuilder.withPayload("not-json").build());
    }

    @Test
    void rawMessageParameterFallsBackToObjectPayloadType() {
        RawProcessor processor = new RawProcessor();
        KafkaClusterProperties props = propsFor("any", "rawHandle", "bytes");

        new MessageHandlerRegistry(List.of(processor), props);
    }

    public static class RawProcessor implements MessageProcessor {
        @SuppressWarnings("rawtypes")
        public void rawHandle(Message message) {
            // raw Message type triggers extractPayloadType fallback path
        }
    }

    @Test
    void handlerExceptionIsLoggedAndDoesNotPropagate() {
        ThrowingProcessor processor = new ThrowingProcessor();
        KafkaClusterProperties props = propsFor("orders", "boom", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);

        registry.getHandler("orders").accept(MessageBuilder.withPayload("{}".getBytes()).build());
    }

    public static class ThrowingProcessor implements MessageProcessor {
        public void boom(Message<Object> message) {
            throw new RuntimeException("handler failed");
        }
    }

    @Test
    void bytesContentTypeWithExplicitByteArrayTargetReturnsRaw() {
        AtomicReference<Object> captured = new AtomicReference<>();
        BytesProcessor processor = new BytesProcessor(captured);
        KafkaClusterProperties props = propsFor("raw", "handleRaw", "json");

        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(processor), props);
        byte[] data = {1, 2, 3};
        registry.getHandler("raw").accept(MessageBuilder.withPayload(data).build());

        assertThat(captured.get()).isInstanceOf(byte[].class);
    }

    private static KafkaClusterProperties propsFor(String topic, String handler, String contentType) {
        KafkaClusterProperties props = new KafkaClusterProperties();
        ConsumerConfig c = new ConsumerConfig();
        c.setTopic(topic);
        c.setHandler(handler);
        c.setContentType(contentType);
        props.setConsumers(List.of(c));
        return props;
    }

    public record Order(String id) {}

    public static class TestProcessor implements MessageProcessor {
        private final AtomicReference<String> sink;

        public TestProcessor(AtomicReference<String> sink) {
            this.sink = sink;
        }

        public void processOrder(Message<Order> message) {
            sink.set(message.getPayload().id());
        }
    }

    public static class StringProcessor implements MessageProcessor {
        private final AtomicReference<Object> sink;

        public StringProcessor(AtomicReference<Object> sink) {
            this.sink = sink;
        }

        public void handleEvent(Message<String> message) {
            sink.set(message.getPayload());
        }
    }

    public static class BytesProcessor implements MessageProcessor {
        private final AtomicReference<Object> sink;

        public BytesProcessor(AtomicReference<Object> sink) {
            this.sink = sink;
        }

        public void handleRaw(Message<byte[]> message) {
            sink.set(message.getPayload());
        }
    }
}
