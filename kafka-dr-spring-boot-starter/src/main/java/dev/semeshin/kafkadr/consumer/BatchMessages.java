package dev.semeshin.kafkadr.consumer;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unpacks the batch envelope Spring Cloud Stream delivers when
 * {@code consumer.batch-mode=true}.
 *
 * <p>{@code BatchMessagingMessageConverter} produces a single message whose payload is
 * a {@code List} and whose Kafka headers are parallel lists — one element per record —
 * plus {@link KafkaHeaders#BATCH_CONVERTED_HEADERS} holding each record's own headers.
 * Records are therefore linked to their metadata by index only.
 *
 * <p>Turning that back into per-record messages is what lets the existing SPIs keep
 * working in batch mode: {@code IdempotencyStore.tryProcess(Message)}, key extraction,
 * watermark tracking and {@code Message<T>} handlers all stay unchanged.
 */
public final class BatchMessages {

    private BatchMessages() {
    }

    /**
     * Coordinates of one record inside a batch, read without materialising a message.
     * Used to advance watermarks in the pass-through mode, where per-record messages
     * are deliberately never built.
     */
    public record RecordRef(String topic, int partition, long timestamp) {
    }

    /** True when this message is a batch envelope rather than a single record. */
    public static boolean isBatch(Message<?> message) {
        return message.getPayload() instanceof List<?>
                && message.getHeaders().containsKey(KafkaHeaders.BATCH_CONVERTED_HEADERS);
    }

    /**
     * Splits a batch envelope into per-record messages, preserving poll order
     * (which is grouped by partition).
     *
     * <p>{@link KafkaHeaders#ACKNOWLEDGMENT} and {@link KafkaHeaders#CONSUMER} are
     * deliberately not copied: acknowledgment is batch-wide, and handing a per-record
     * message an {@code Acknowledgment} would suggest a single record can be committed
     * on its own. Offsets are a per-partition watermark, so that is never true.
     */
    public static List<Message<?>> split(Message<?> batch) {
        List<?> payloads = (List<?>) batch.getPayload();
        MessageHeaders headers = batch.getHeaders();

        List<?> keys = list(headers, KafkaHeaders.RECEIVED_KEY);
        List<?> topics = list(headers, KafkaHeaders.RECEIVED_TOPIC);
        List<?> partitions = list(headers, KafkaHeaders.RECEIVED_PARTITION);
        List<?> offsets = list(headers, KafkaHeaders.OFFSET);
        List<?> timestamps = list(headers, KafkaHeaders.RECEIVED_TIMESTAMP);
        List<?> perRecordHeaders = list(headers, KafkaHeaders.BATCH_CONVERTED_HEADERS);

        List<Message<?>> records = new ArrayList<>(payloads.size());
        for (int i = 0; i < payloads.size(); i++) {
            // A record the deserializer could not read arrives with no value. Messages cannot
            // hold null, so it is carried as KafkaNull and rejected later by conversion, which
            // knows the record's position and can commit everything before it.
            Object payload = payloads.get(i) == null ? KafkaNull.INSTANCE : payloads.get(i);
            MessageBuilder<?> builder = MessageBuilder.withPayload(payload);

            Object own = element(perRecordHeaders, i);
            if (own instanceof Map<?, ?> map) {
                map.forEach((k, v) -> builder.setHeader(String.valueOf(k), v));
            }

            setIfPresent(builder, KafkaHeaders.RECEIVED_KEY, keys, i);
            setIfPresent(builder, KafkaHeaders.RECEIVED_TOPIC, topics, i);
            setIfPresent(builder, KafkaHeaders.RECEIVED_PARTITION, partitions, i);
            setIfPresent(builder, KafkaHeaders.OFFSET, offsets, i);
            setIfPresent(builder, KafkaHeaders.RECEIVED_TIMESTAMP, timestamps, i);

            records.add(builder.build());
        }
        return records;
    }

    /**
     * Reads topic, partition and timestamp for every record in a batch envelope.
     * Records missing any of the three are skipped — their watermark cannot be placed.
     */
    public static List<RecordRef> coordinates(Message<?> batch) {
        MessageHeaders headers = batch.getHeaders();
        List<?> topics = list(headers, KafkaHeaders.RECEIVED_TOPIC);
        List<?> partitions = list(headers, KafkaHeaders.RECEIVED_PARTITION);
        List<?> timestamps = list(headers, KafkaHeaders.RECEIVED_TIMESTAMP);

        int size = ((List<?>) batch.getPayload()).size();
        List<RecordRef> refs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (element(topics, i) instanceof String topic
                    && element(partitions, i) instanceof Number partition
                    && element(timestamps, i) instanceof Number timestamp) {
                refs.add(new RecordRef(topic, partition.intValue(), timestamp.longValue()));
            }
        }
        return refs;
    }

    private static List<?> list(MessageHeaders headers, String name) {
        return headers.get(name) instanceof List<?> list ? list : null;
    }

    private static Object element(List<?> list, int index) {
        return (list != null && index < list.size()) ? list.get(index) : null;
    }

    private static void setIfPresent(MessageBuilder<?> builder, String header, List<?> values, int index) {
        Object value = element(values, index);
        if (value != null) {
            builder.setHeader(header, value);
        }
    }
}
