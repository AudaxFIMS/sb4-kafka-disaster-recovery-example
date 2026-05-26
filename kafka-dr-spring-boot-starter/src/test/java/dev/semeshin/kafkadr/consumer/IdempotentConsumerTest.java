package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotentConsumerTest {

    private IdempotencyStore store;
    private LastProcessedTimestampTracker tracker;
    private AtomicInteger delegateCount;
    private Consumer<Message<?>> delegate;

    @BeforeEach
    void setup() {
        store = mock(IdempotencyStore.class);
        tracker = new LastProcessedTimestampTracker(null);
        delegateCount = new AtomicInteger(0);
        delegate = msg -> delegateCount.incrementAndGet();
    }

    @Test
    void byteArrayReceivedKeyIsConvertedToUtf8String() {
        when(store.tryProcess(eq("orders"), eq("o-1"))).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1".getBytes())
                .build());

        verify(store).tryProcess("orders", "o-1");
        assertThat(delegateCount.get()).isEqualTo(1);
    }

    @Test
    void stringReceivedKeyIsUsedAsIs() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .build());

        verify(store).tryProcess("orders", "o-1");
    }

    @Test
    void duplicateSkipsDelegate() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(false);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .build());

        assertThat(delegateCount.get()).isZero();
    }

    @Test
    void missingKeyCallsDelegateWithoutCheck() {
        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);

        c.accept(MessageBuilder.withPayload("payload").build());

        verify(store, never()).tryProcess(anyString(), anyString());
        assertThat(delegateCount.get()).isEqualTo(1);
    }

    @Test
    void customKeyHeaderTakesPrecedenceOverKafkaKey() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, "x-idempotency-key", tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "kafka-key")
                .setHeader("x-idempotency-key", "custom-key")
                .build());

        verify(store).tryProcess("orders", "custom-key");
    }

    @Test
    void fallsBackToProducerSideKafkaKeyWhenReceivedKeyAbsent() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.KEY, "k-1")
                .build());

        verify(store).tryProcess("orders", "k-1");
    }

    @Test
    void receivedTimestampUpdatesTracker() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build());

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(1714003200000L);
    }

    @Test
    void timestampNotTrackedOnDuplicate() {
        when(store.tryProcess(anyString(), anyString())).thenReturn(false);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, null, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build());

        assertThat(tracker.getLastTimestamp("orders")).isNull();
    }
}
