package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.Shape;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class MessageHandlerRegistryBatchTest {

    record Order(String id, int amount) {
        Order() { this(null, 0); }
    }

    /** One bean carrying every supported handler shape. */
    static class Handlers implements MessageProcessor {
        final List<Object> seen = new ArrayList<>();

        public void single(Message<Order> message) {
            seen.add(message.getPayload());
        }

        public void listOfMessage(List<Message<Order>> messages) {
            messages.forEach(m -> seen.add(m.getPayload()));
        }

        public void messageOfList(Message<List<Order>> batch) {
            seen.add(batch);
        }

        public void listOfPayload(List<Order> orders) {
            seen.addAll(orders);
        }

        public void alwaysFails(Message<Order> message) {
            throw new IllegalStateException("boom on " + message.getPayload());
        }

        public void batchAlwaysFails(List<Message<Order>> messages) {
            throw new IllegalStateException("whole batch failed");
        }
    }

    // --- shapes ---------------------------------------------------------------

    @Test
    void allFourHandlerShapesAreRecognised() {
        assertThat(registry(consumer("c", "single", split())).shapeOf("c")).isEqualTo(Shape.SINGLE);
        assertThat(registry(consumer("c", "listOfMessage", split())).shapeOf("c"))
                .isEqualTo(Shape.LIST_OF_MESSAGE);
        assertThat(registry(consumer("c", "messageOfList", standard())).shapeOf("c"))
                .isEqualTo(Shape.MESSAGE_OF_LIST);
        assertThat(registry(consumer("c", "listOfPayload", standard())).shapeOf("c"))
                .isEqualTo(Shape.LIST_OF_PAYLOAD);
    }

    @Test
    void missingHandlerMethodNamesTheAcceptedSignatures() {
        assertThatThrownBy(() -> registry(consumer("c", "noSuchMethod", split())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Message<T>, List<Message<T>>, Message<List<T>> or List<T>");
    }

    // --- conversion -----------------------------------------------------------

    @Test
    void perRecordDriverConvertsEachElement() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "single", split()));

        BatchHandler.Result result = registry.getBatchHandler("c")
                .process(List.of(json("{\"id\":\"a\",\"amount\":1}"), json("{\"id\":\"b\",\"amount\":2}")));

        assertThat(result.stopped()).isFalse();
        assertThat(handlers.seen).containsExactly(new Order("a", 1), new Order("b", 2));
    }

    @Test
    void listOfMessageDriverConvertsBeforeTheSingleCall() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "listOfMessage", split()));

        registry.getBatchHandler("c")
                .process(List.of(json("{\"id\":\"a\",\"amount\":1}"), json("{\"id\":\"b\",\"amount\":2}")));

        assertThat(handlers.seen).containsExactly(new Order("a", 1), new Order("b", 2));
    }

    @Test
    void envelopeHandlerConvertsElementsAndKeepsHeaders() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "messageOfList", standard()));
        Acknowledgment ack = mock(Acknowledgment.class);

        Message<?> envelope = MessageBuilder
                .withPayload(List.of(bytes("{\"id\":\"a\",\"amount\":1}"), bytes("{\"id\":\"b\",\"amount\":2}")))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of(), Map.of()))
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();

        registry.getEnvelopeHandler("c").accept(envelope);

        Message<?> delivered = (Message<?>) handlers.seen.get(0);
        assertThat(delivered.getPayload()).isEqualTo(List.of(new Order("a", 1), new Order("b", 2)));
        // The handler owns acknowledgment in standard mode, so conversion must not drop it.
        assertThat(delivered.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
        assertThat(delivered.getHeaders()).containsKey(KafkaHeaders.BATCH_CONVERTED_HEADERS);
    }

    @Test
    void listOfPayloadHandlerReceivesConvertedElementsOnly() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "listOfPayload", standard()));

        registry.getEnvelopeHandler("c").accept(MessageBuilder
                .withPayload(List.of(bytes("{\"id\":\"a\",\"amount\":1}")))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of()))
                .build());

        assertThat(handlers.seen).containsExactly(new Order("a", 1));
    }

    @Test
    void malformedElementFailsWithItsIndexInsteadOfBecomingAString() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "single", split()));

        BatchHandler.Result result = registry.getBatchHandler("c").process(List.of(
                json("{\"id\":\"a\",\"amount\":1}"),
                json("not json at all"),
                json("{\"id\":\"c\",\"amount\":3}")));

        // Substituting a String would put a foreign type into List<Order> and surface as a
        // ClassCastException deep inside business logic instead.
        assertThat(result.stopped()).isTrue();
        assertThat(result.firstUnprocessedIndex()).isEqualTo(1);
        assertThat(result.failure()).isInstanceOf(BatchConversionException.class);
        assertThat(handlers.seen).containsExactly(new Order("a", 1));
    }

    @Test
    void unreadableRecordFromErrorHandlingDeserializerIsReportedNotPassedOn() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "single", split()));

        // A record the deserializer could not read carries no value. Batch mode never runs
        // checkDeser, so unlike record mode it reaches the listener.
        Message<?> unreadable = BatchMessages.split(MessageBuilder
                .withPayload(nullableList(null))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of()))
                .build()).get(0);

        BatchHandler.Result result = registry.getBatchHandler("c")
                .process(List.of(json("{\"id\":\"a\",\"amount\":1}"), unreadable));

        assertThat(result.stopped()).isTrue();
        assertThat(result.firstUnprocessedIndex()).isEqualTo(1);
        assertThat(result.failure())
                .isInstanceOf(BatchConversionException.class)
                .hasMessageContaining("DeserializationException header");
        assertThat(handlers.seen).containsExactly(new Order("a", 1));
    }

    @Test
    void unconvertiblePayloadTypeFailsInsteadOfReachingTheHandler() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "single", split()));

        BatchHandler.Result result = registry.getBatchHandler("c")
                .process(List.of(MessageBuilder.withPayload(42).build()));

        assertThat(result.failure())
                .isInstanceOf(BatchConversionException.class)
                .hasMessageContaining("ClassCastException");
    }

    @Test
    void nativeContentTypeSkipsConversion() {
        Handlers handlers = new Handlers();
        ConsumerConfig config = consumer("c", "single", split());
        config.setContentType("native");
        MessageHandlerRegistry registry = registry(handlers, config);

        Order alreadyTyped = new Order("a", 1);
        registry.getBatchHandler("c").process(List.of(MessageBuilder.withPayload(alreadyTyped).build()));

        assertThat(handlers.seen).containsExactly(alreadyTyped);
    }

    // --- error policy ---------------------------------------------------------

    @Test
    void failBatchStopsAtTheFirstFailure() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "alwaysFails", split()));

        BatchHandler.Result result = registry.getBatchHandler("c")
                .process(List.of(json("{\"id\":\"a\",\"amount\":1}"), json("{\"id\":\"b\",\"amount\":2}")));

        assertThat(result.firstUnprocessedIndex()).isZero();
        assertThat(result.rollback()).hasSize(2);
        assertThat(result.failure()).hasMessageContaining("boom");
    }

    @Test
    void skipFailedReportsEveryPassedOverRecord() {
        Handlers handlers = new Handlers();
        BatchConfig batch = split();
        batch.setErrorPolicy(BatchConfig.ErrorPolicy.SKIP_FAILED);
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "alwaysFails", batch));

        Message<?> first = json("{\"id\":\"a\",\"amount\":1}");
        Message<?> second = json("{\"id\":\"b\",\"amount\":2}");
        BatchHandler.Result result = registry.getBatchHandler("c").process(List.of(first, second));

        // The consumer rolls these back so a redelivery is not treated as a duplicate.
        assertThat(result.stopped()).isFalse();
        assertThat(result.rollback()).hasSize(2);
    }

    @Test
    void listOfMessageFailureRestartsTheWholeBatch() {
        Handlers handlers = new Handlers();
        MessageHandlerRegistry registry = registry(handlers, consumer("c", "batchAlwaysFails", split()));

        BatchHandler.Result result = registry.getBatchHandler("c")
                .process(List.of(json("{\"id\":\"a\",\"amount\":1}"), json("{\"id\":\"b\",\"amount\":2}")));

        // One call for the whole list: which record failed is unknowable.
        assertThat(result.firstUnprocessedIndex()).isZero();
        assertThat(result.failure()).hasMessageContaining("whole batch failed");
    }

    // --- validation -----------------------------------------------------------

    @Test
    void batchHandlerWithoutBatchingIsRejected() {
        assertThatThrownBy(() -> registry(consumer("c", "listOfMessage", new BatchConfig())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("batching is off");
    }

    @Test
    void splitModeRejectsAnEnvelopeHandler() {
        assertThatThrownBy(() -> registry(consumer("c", "messageOfList", split())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mode=split");
    }

    @Test
    void standardModeRejectsAPerRecordHandler() {
        assertThatThrownBy(() -> registry(consumer("c", "single", standard())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mode=standard");
    }

    @Test
    void skipFailedWithAListHandlerIsRejected() {
        BatchConfig batch = split();
        batch.setErrorPolicy(BatchConfig.ErrorPolicy.SKIP_FAILED);

        // The handler is called once for the whole list, so a single record cannot be skipped.
        assertThatThrownBy(() -> registry(consumer("c", "listOfMessage", batch)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be skipped");
    }

    @Test
    void consumerWithoutHandlerFallsBackToLoggingTheBatch() {
        ConsumerConfig config = consumer("c", null, split());
        MessageHandlerRegistry registry = registry(new Handlers(), config);

        assertThat(registry.getBatchHandler("c").process(List.of(json("{}"))))
                .isEqualTo(BatchHandler.Result.COMPLETE);
    }

    // --- fixtures -------------------------------------------------------------

    private static MessageHandlerRegistry registry(ConsumerConfig consumer) {
        return registry(new Handlers(), consumer);
    }

    private static MessageHandlerRegistry registry(Handlers handlers, ConsumerConfig consumer) {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        Map<String, ConsumerConfig> map = new LinkedHashMap<>();
        map.put(consumer.getName(), consumer);
        properties.setConsumers(map);
        return new MessageHandlerRegistry(List.of(handlers), properties);
    }

    private static ConsumerConfig consumer(String name, String handler, BatchConfig batch) {
        ConsumerConfig config = new ConsumerConfig();
        config.setName(name);
        config.setTopic("orders");
        config.setHandler(handler);
        config.setBatch(batch);
        return config;
    }

    private static BatchConfig split() {
        BatchConfig batch = new BatchConfig();
        batch.setEnabled(true);
        return batch;
    }

    private static BatchConfig standard() {
        BatchConfig batch = split();
        batch.setMode(BatchConfig.Mode.STANDARD);
        return batch;
    }

    private static Message<?> json(String body) {
        return MessageBuilder.withPayload(bytes(body)).build();
    }

    private static List<Object> nullableList(Object element) {
        List<Object> list = new ArrayList<>();
        list.add(element);
        return list;
    }

    private static byte[] bytes(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
