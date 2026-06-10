package dev.semeshin.kafkadr.idempotency;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryIdempotencyStoreTest {

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();

    private static Message<?> messageWithKey(String key) {
        return MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, key)
                .build();
    }

    @Test
    void firstCallReturnsTrue() {
        assertThat(store.tryProcess("orders-consumer", messageWithKey("msg-1"))).isTrue();
    }

    @Test
    void duplicateCallReturnsFalse() {
        store.tryProcess("orders-consumer", messageWithKey("msg-1"));
        assertThat(store.tryProcess("orders-consumer", messageWithKey("msg-1"))).isFalse();
    }

    @Test
    void differentMessagesForSameConsumerAreIndependent() {
        assertThat(store.tryProcess("orders-consumer", messageWithKey("msg-1"))).isTrue();
        assertThat(store.tryProcess("orders-consumer", messageWithKey("msg-2"))).isTrue();
    }

    @Test
    void sameKeyAcrossDifferentConsumersIsNotADuplicate() {
        store.tryProcess("orders-consumer", messageWithKey("shared-id"));
        assertThat(store.tryProcess("payments-consumer", messageWithKey("shared-id"))).isTrue();
    }

    @Test
    void messageWithoutKeyIsAlwaysProcessed() {
        Message<?> noKey = MessageBuilder.withPayload("payload").build();
        assertThat(store.tryProcess("orders-consumer", noKey)).isTrue();
        assertThat(store.tryProcess("orders-consumer", noKey)).isTrue();
    }

    @Test
    void byteArrayReceivedKeyIsConvertedToUtf8String() {
        Message<?> bytesKey = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1".getBytes())
                .build();
        store.tryProcess("orders-consumer", bytesKey);
        assertThat(store.tryProcess("orders-consumer", messageWithKey("o-1"))).isFalse();
    }

    @Test
    void fallsBackToProducerSideKafkaKeyWhenReceivedKeyAbsent() {
        Message<?> producerKey = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.KEY, "k-1")
                .build();
        store.tryProcess("orders-consumer", producerKey);
        assertThat(store.tryProcess("orders-consumer", messageWithKey("k-1"))).isFalse();
    }

    @Test
    void customKeyHeaderTakesPrecedenceOverKafkaKey() {
        InMemoryIdempotencyStore headerStore = new InMemoryIdempotencyStore("x-idempotency-key");
        Message<?> msg = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "kafka-key")
                .setHeader("x-idempotency-key", "custom-key")
                .build();

        assertThat(headerStore.tryProcess("orders-consumer", msg)).isTrue();

        // same custom header but different kafka key — still a duplicate
        Message<?> sameHeaderDifferentKafkaKey = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "other-kafka-key")
                .setHeader("x-idempotency-key", "custom-key")
                .build();
        assertThat(headerStore.tryProcess("orders-consumer", sameHeaderDifferentKafkaKey)).isFalse();
    }

    @Test
    void overridingExtractKeyCustomizesDeduplication() {
        // Custom extraction: dedup by payload content instead of the Kafka key
        InMemoryIdempotencyStore payloadStore = new InMemoryIdempotencyStore() {
            @Override
            public String extractKey(String consumerName, Message<?> message) {
                return message.getPayload().toString();
            }
        };

        Message<?> first = MessageBuilder.withPayload("order-42")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-a")
                .build();
        Message<?> samePayloadDifferentKey = MessageBuilder.withPayload("order-42")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-b")
                .build();

        assertThat(payloadStore.tryProcess("c1", first)).isTrue();
        assertThat(payloadStore.tryProcess("c1", samePayloadDifferentKey)).isFalse();
    }

    @Test
    void evictExpiredKeepsRecentEntries() {
        store.tryProcess("c1", messageWithKey("fresh-1"));
        store.tryProcess("c1", messageWithKey("fresh-2"));

        store.evictExpired();

        assertThat(store.tryProcess("c1", messageWithKey("fresh-1"))).isFalse();
        assertThat(store.tryProcess("c1", messageWithKey("fresh-2"))).isFalse();
    }

    @Test
    void evictExpiredRemovesEntriesOlderThanTtl() {
        ConcurrentHashMap<String, Instant> entries = stateOf(store);
        entries.put("c1:old-1", Instant.now().minusSeconds(7200));
        entries.put("c1:old-2", Instant.now().minusSeconds(7200));
        entries.put("c1:fresh", Instant.now());

        store.evictExpired();

        assertThat(store.tryProcess("c1", messageWithKey("old-1"))).isTrue();
        assertThat(store.tryProcess("c1", messageWithKey("old-2"))).isTrue();
        assertThat(store.tryProcess("c1", messageWithKey("fresh"))).isFalse();
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Instant> stateOf(InMemoryIdempotencyStore store) {
        return (ConcurrentHashMap<String, Instant>) ReflectionTestUtils.getField(store, "processedIds");
    }
}
