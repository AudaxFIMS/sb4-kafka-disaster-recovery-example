package dev.semeshin.kafkadr.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class BatchPassThroughConsumerTest {

    private LastProcessedTimestampTracker tracker;
    private List<Message<?>> seen;
    private Consumer<Message<?>> delegate;

    @BeforeEach
    void setup() {
        tracker = new LastProcessedTimestampTracker(null);
        seen = new ArrayList<>();
        delegate = seen::add;
    }

    @Test
    void envelopeReachesTheHandlerUntouched() {
        Acknowledgment ack = mock(Acknowledgment.class);
        Message<?> envelope = MessageBuilder.fromMessage(batch()).setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack).build();

        consumer().accept(envelope);

        assertThat(seen).hasSize(1);
        Message<?> delivered = seen.get(0);
        assertThat(delivered.getPayload()).isEqualTo(List.of("a", "b"));
        // The handler owns acknowledgment in this mode, so the header must survive.
        assertThat(delivered.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
        assertThat(delivered.getHeaders()).containsKey(KafkaHeaders.BATCH_CONVERTED_HEADERS);
    }

    @Test
    void watermarkTakesTheMaximumPerPartition() {
        consumer().accept(BatchMessagesTest.envelope(
                List.of("a", "b", "c"),
                List.of("k-1", "k-2", "k-3"),
                List.of("orders", "orders", "orders"),
                List.of(0, 1, 0),
                List.of(10L, 20L, 11L),
                List.of(1000L, 5000L, 3000L),
                List.of(Map.of(), Map.of(), Map.of())));

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(3000L);
        assertThat(tracker.getLastTimestamp("orders", 1)).isEqualTo(5000L);
    }

    @Test
    void handlerExceptionsPropagateAndLeaveTheWatermarkAlone() {
        RuntimeException boom = new IllegalStateException("boom");
        delegate = msg -> { throw boom; };

        assertThatThrownBy(() -> consumer().accept(batch())).isSameAs(boom);

        // Error handling belongs to the handler here, but a failed batch must not
        // advance the watermark past records that will be redelivered.
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void recordModeEnvelopeStillAdvancesTheWatermark() {
        Message<?> single = MessageBuilder.withPayload("solo")
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "orders")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1000L)
                .build();

        consumer().accept(single);

        assertThat(seen).hasSize(1);
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1000L);
    }

    @Test
    void manualAckModeHoldsTheWatermarkUntilTheHandlerAcknowledges() {
        Acknowledgment ack = mock(Acknowledgment.class);
        Message<?> envelope = MessageBuilder.fromMessage(batch())
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack).build();

        manualConsumer().accept(envelope);

        // The handler ignored the acknowledgment, so nothing was committed and the watermark
        // must not run ahead of the committed offset.
        verify(ack, never()).acknowledge();
        assertThat(tracker.getAllTimestamps()).isEmpty();
    }

    @Test
    void manualAckModeAdvancesTheWatermarkOnceTheHandlerAcknowledges() {
        Acknowledgment ack = mock(Acknowledgment.class);
        Message<?> envelope = MessageBuilder.fromMessage(batch())
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack).build();
        delegate = message -> {
            seen.add(message);
            message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class).acknowledge();
        };

        manualConsumer().accept(envelope);

        verify(ack).acknowledge();
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(2000L);
        // Wrapping the acknowledgment rebuilds the envelope, which must keep everything else.
        assertThat(seen.get(0).getPayload()).isEqualTo(List.of("a", "b"));
        assertThat(seen.get(0).getHeaders()).containsKey(KafkaHeaders.BATCH_CONVERTED_HEADERS);
    }

    private BatchPassThroughConsumer manualConsumer() {
        return new BatchPassThroughConsumer("orders", "primary", delegate, tracker,
                new AckPolicy(AckMode.MANUAL, AckPolicy.Owner.HANDLER, false));
    }

    private BatchPassThroughConsumer consumer() {
        return new BatchPassThroughConsumer("orders", "primary", delegate, tracker);
    }

    private static Message<?> batch() {
        return BatchMessagesTest.envelope(
                List.of("a", "b"),
                List.of("k-1", "k-2"),
                List.of("orders", "orders"),
                List.of(0, 0),
                List.of(10L, 11L),
                List.of(1000L, 2000L),
                List.of(Map.of(), Map.of()));
    }
}
