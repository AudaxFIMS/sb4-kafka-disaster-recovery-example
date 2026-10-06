package dev.semeshin.kafkadr.consumer;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LastProcessedTimestampTrackerTest {

    @Test
    void withoutStoreTracksInMemoryOnly() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 0, 1000L);

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1000L);
    }

    @Test
    void newerTimestampReplacesOlder() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 0, 1000L);
        tracker.update("orders", 0, 500L);
        tracker.update("orders", 0, 2000L);

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(2000L);
    }

    @Test
    void olderTimestampDoesNotOverwrite() {
        TimestampStore store = mock(TimestampStore.class);
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
        tracker.update("orders", 0, 1000L);

        tracker.update("orders", 0, 500L);

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1000L);
        verify(store, times(1)).save(anyString(), anyLong());
        verify(store).save("orders-0", 1000L);
    }

    @Test
    void updateDelegatesToStoreOnNewerValue() {
        TimestampStore store = mock(TimestampStore.class);
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);

        tracker.update("orders", 0, 1000L);
        tracker.update("orders", 0, 2000L);

        verify(store).save("orders-0", 1000L);
        verify(store).save("orders-0", 2000L);
    }

    @Test
    void partitionsOfTheSameTopicAreTrackedSeparately() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);

        tracker.update("orders", 0, 5000L);
        tracker.update("orders", 1, 1000L);

        // A per-topic watermark would report 5000 for partition 1 and seek past
        // records it never processed.
        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(5000L);
        assertThat(tracker.getLastTimestamp("orders", 1)).isEqualTo(1000L);
    }

    @Test
    void restoreLoadsExistingStoreContents() {
        TimestampStore store = mock(TimestampStore.class);
        when(store.loadAll()).thenReturn(Map.of("orders-0", 5000L, "payments-3", 7000L));

        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
        tracker.restore();

        assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(5000L);
        assertThat(tracker.getLastTimestamp("payments", 3)).isEqualTo(7000L);
    }

    @Test
    void getAllTimestampsReturnsImmutableCopy() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 0, 1000L);
        tracker.update("payments", 0, 2000L);

        Map<String, Long> snapshot = tracker.getAllTimestamps();

        assertThat(snapshot).containsExactlyInAnyOrderEntriesOf(
                Map.of("orders-0", 1000L, "payments-0", 2000L));
    }

    @Test
    void concurrentUpdatesKeepTheHighestTimestamp() throws Exception {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        int threads = 8;
        int perThread = 500;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int t = 0; t < threads; t++) {
                final int offset = t;
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // Interleaved ascending and descending sequences: a
                        // read-compare-write would let a lower value win a race.
                        tracker.update("orders", 0, 1000L + (long) i * threads + offset);
                        tracker.update("orders", 0, 1000L + (long) (perThread - i) * threads);
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        long expectedMax = 1000L + (long) (perThread - 1) * threads + (threads - 1);
        assertThat(tracker.getLastTimestamp("orders", 0)).isGreaterThanOrEqualTo(expectedMax);
    }
}
