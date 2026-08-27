package dev.semeshin.kafkadr.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class BatchMessagesTest {

    @Test
    void singleRecordMessageIsNotABatch() {
        Message<?> record = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .build();

        assertThat(BatchMessages.isBatch(record)).isFalse();
    }

    @Test
    void listPayloadWithoutConvertedHeadersIsNotABatch() {
        // A handler could legitimately produce a List payload; only the converter's
        // marker header identifies a real batch envelope.
        Message<?> notABatch = MessageBuilder.withPayload(List.of("a", "b")).build();

        assertThat(BatchMessages.isBatch(notABatch)).isFalse();
    }

    @Test
    void splitRebuildsPerRecordMessagesInPollOrder() {
        Message<?> envelope = envelope(
                List.of("first", "second", "third"),
                List.of("k-1", "k-2", "k-3"),
                List.of("orders", "orders", "orders"),
                List.of(0, 0, 1),
                List.of(10L, 11L, 40L),
                List.of(1000L, 1001L, 1002L),
                List.of(Map.of("x-trace", "t-1"), Map.of("x-trace", "t-2"), Map.of("x-trace", "t-3")));

        List<Message<?>> records = BatchMessages.split(envelope);

        assertThat(records).hasSize(3);
        assertThat(records.stream().map(m -> (Object) m.getPayload()).toList())
                .containsExactly("first", "second", "third");
        assertThat(records.get(2).getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("k-3");
        assertThat(records.get(2).getHeaders().get(KafkaHeaders.RECEIVED_PARTITION)).isEqualTo(1);
        assertThat(records.get(2).getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(40L);
        assertThat(records.get(2).getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1002L);
        assertThat(records.get(1).getHeaders().get("x-trace")).isEqualTo("t-2");
    }

    @Test
    void acknowledgmentDoesNotLeakIntoPerRecordMessages() {
        Message<?> envelope = MessageBuilder.withPayload(List.of("a", "b"))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of(), Map.of()))
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, mock(Acknowledgment.class))
                .setHeader(KafkaHeaders.CONSUMER, new Object())
                .build();

        List<Message<?>> records = BatchMessages.split(envelope);

        // Offsets are a per-partition watermark: a per-record Acknowledgment would
        // suggest one record can be committed alone, which is never true.
        assertThat(records).allSatisfy(record -> {
            assertThat(record.getHeaders()).doesNotContainKey(KafkaHeaders.ACKNOWLEDGMENT);
            assertThat(record.getHeaders()).doesNotContainKey(KafkaHeaders.CONSUMER);
        });
    }

    @Test
    void splitToleratesMissingHeaderLists() {
        Message<?> envelope = MessageBuilder.withPayload(List.of("a", "b"))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of(), Map.of()))
                .build();

        List<Message<?>> records = BatchMessages.split(envelope);

        assertThat(records).hasSize(2);
        assertThat(records.get(0).getHeaders()).doesNotContainKey(KafkaHeaders.RECEIVED_TOPIC);
    }

    @Test
    void splitToleratesShorterHeaderListsThanPayloads() {
        Message<?> envelope = MessageBuilder.withPayload(List.of("a", "b", "c"))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of()))
                .setHeader(KafkaHeaders.RECEIVED_KEY, List.of("k-1"))
                .build();

        List<Message<?>> records = BatchMessages.split(envelope);

        assertThat(records).hasSize(3);
        assertThat(records.get(0).getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("k-1");
        assertThat(records.get(2).getHeaders()).doesNotContainKey(KafkaHeaders.RECEIVED_KEY);
    }

    @Test
    void emptyBatchSplitsToNothing() {
        Message<?> envelope = MessageBuilder.withPayload(List.of())
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of())
                .build();

        assertThat(BatchMessages.split(envelope)).isEmpty();
    }

    @Test
    void coordinatesReadTopicPartitionAndTimestampPerRecord() {
        Message<?> envelope = envelope(
                List.of("a", "b"),
                List.of("k-1", "k-2"),
                List.of("orders", "orders"),
                List.of(0, 3),
                List.of(10L, 11L),
                List.of(1000L, 2000L),
                List.of(Map.of(), Map.of()));

        assertThat(BatchMessages.coordinates(envelope)).containsExactly(
                new BatchMessages.RecordRef("orders", 0, 1000L),
                new BatchMessages.RecordRef("orders", 3, 2000L));
    }

    @Test
    void coordinatesSkipRecordsWithIncompleteMetadata() {
        Message<?> envelope = MessageBuilder.withPayload(List.of("a", "b"))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(Map.of(), Map.of()))
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, List.of("orders"))
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, List.of(0))
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, List.of(1000L))
                .build();

        // The second record has no metadata, so its watermark cannot be placed.
        assertThat(BatchMessages.coordinates(envelope))
                .containsExactly(new BatchMessages.RecordRef("orders", 0, 1000L));
    }

    static Message<?> envelope(List<?> payloads, List<?> keys, List<?> topics, List<?> partitions,
                               List<?> offsets, List<?> timestamps, List<?> convertedHeaders) {
        return MessageBuilder.withPayload(payloads)
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, convertedHeaders)
                .setHeader(KafkaHeaders.RECEIVED_KEY, keys)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, topics)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, partitions)
                .setHeader(KafkaHeaders.OFFSET, offsets)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, timestamps)
                .build();
    }
}
