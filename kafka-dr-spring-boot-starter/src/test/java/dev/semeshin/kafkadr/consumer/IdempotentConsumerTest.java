package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
    void firstTimeMessageCallsDelegate() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .build());

        assertThat(delegateCount.get()).isEqualTo(1);
    }

    @Test
    void duplicateSkipsDelegate() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(false);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .build());

        assertThat(delegateCount.get()).isZero();
    }

    @Test
    void storeReceivesConsumerNameAndFullMessage() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader("x-custom", "custom-value")
                .build());

        ArgumentCaptor<Message<?>> captor = ArgumentCaptor.captor();
        verify(store).tryProcess(eq("primary"), eq("orders"), captor.capture());
        Message<?> seen = captor.getValue();
        assertThat(seen.getPayload()).isEqualTo("payload");
        assertThat(seen.getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("o-1");
        assertThat(seen.getHeaders().get("x-custom")).isEqualTo("custom-value");
    }

    @Test
    void messageWithoutKeyIsStillPassedToStore() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload").build());

        verify(store).tryProcess(eq("primary"), eq("orders"), any());
        assertThat(delegateCount.get()).isEqualTo(1);
    }

    @Test
    void receivedTimestampUpdatesTracker() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(true);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build());

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(1714003200000L);
    }

    @Test
    void disabledStoreProcessesEveryMessageAndStillTracksTimestamp() {
        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", IdempotencyStore.DISABLED, delegate, tracker);
        Message<?> msg = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build();

        c.accept(msg);
        c.accept(msg);

        assertThat(delegateCount.get()).isEqualTo(2);
        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(1714003200000L);
    }

    @Test
    void timestampNotTrackedOnDuplicate() {
        when(store.tryProcess(anyString(), anyString(), any())).thenReturn(false);

        IdempotentConsumer c = new IdempotentConsumer(
                "orders", "primary", store, delegate, tracker);
        c.accept(MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, "o-1")
                .setHeader(KafkaHeaders.RECEIVED_TIMESTAMP, 1714003200000L)
                .build());

        assertThat(tracker.getLastTimestamp("orders")).isNull();
    }
}
