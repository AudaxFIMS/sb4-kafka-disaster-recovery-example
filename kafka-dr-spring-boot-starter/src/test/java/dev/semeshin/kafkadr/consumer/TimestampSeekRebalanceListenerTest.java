package dev.semeshin.kafkadr.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TimestampSeekRebalanceListenerTest {

    @Test
    @SuppressWarnings("unchecked")
    void seeksToOffsetMatchingLastTimestamp() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 1714003200000L);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        TopicPartition tp = new TopicPartition("orders", 0);
        when(consumer.offsetsForTimes(any())).thenReturn(Map.of(
                tp, new OffsetAndTimestamp(1542L, 1714003200000L)
        ));

        new TimestampSeekRebalanceListener(tracker)
                .onPartitionsAssigned(consumer, List.of(tp));

        verify(consumer).seek(tp, 1542L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void skipsSeekWhenNoOffsetFoundForTimestamp() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 1714003200000L);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        TopicPartition tp = new TopicPartition("orders", 0);
        Map<TopicPartition, OffsetAndTimestamp> nullEntry = new HashMap<>();
        nullEntry.put(tp, null);
        when(consumer.offsetsForTimes(any())).thenReturn(nullEntry);

        new TimestampSeekRebalanceListener(tracker)
                .onPartitionsAssigned(consumer, List.of(tp));

        verify(consumer, never()).seek(any(), anyLong());
    }

    @Test
    @SuppressWarnings("unchecked")
    void doesNothingWhenNoTimestampForTopic() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        TopicPartition tp = new TopicPartition("unknown", 0);

        new TimestampSeekRebalanceListener(tracker)
                .onPartitionsAssigned(consumer, List.of(tp));

        verify(consumer, never()).offsetsForTimes(any());
        verify(consumer, never()).seek(any(), anyLong());
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyPartitionsAssignmentIsNoOp() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        Consumer<Object, Object> consumer = mock(Consumer.class);

        new TimestampSeekRebalanceListener(tracker)
                .onPartitionsAssigned(consumer, List.of());

        verify(consumer, never()).offsetsForTimes(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void seeksMultiplePartitionsIndependently() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 1000L);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);
        when(consumer.offsetsForTimes(any())).thenReturn(Map.of(
                tp0, new OffsetAndTimestamp(10L, 1000L),
                tp1, new OffsetAndTimestamp(20L, 1000L)
        ));

        new TimestampSeekRebalanceListener(tracker)
                .onPartitionsAssigned(consumer, List.of(tp0, tp1));

        verify(consumer).seek(tp0, 10L);
        verify(consumer).seek(tp1, 20L);
        verify(consumer, times(2)).seek(any(), anyLong());
    }
}
