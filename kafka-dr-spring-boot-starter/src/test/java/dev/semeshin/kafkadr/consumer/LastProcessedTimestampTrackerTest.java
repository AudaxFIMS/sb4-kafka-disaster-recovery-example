package dev.semeshin.kafkadr.consumer;

import org.junit.jupiter.api.Test;

import java.util.Map;

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
        tracker.update("orders", 1000L);

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(1000L);
    }

    @Test
    void newerTimestampReplacesOlder() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 1000L);
        tracker.update("orders", 500L);
        tracker.update("orders", 2000L);

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(2000L);
    }

    @Test
    void olderTimestampDoesNotOverwrite() {
        TimestampStore store = mock(TimestampStore.class);
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
        tracker.update("orders", 1000L);

        tracker.update("orders", 500L);

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(1000L);
        verify(store, times(1)).save(anyString(), anyLong());
        verify(store).save("orders", 1000L);
    }

    @Test
    void updateDelegatesToStoreOnNewerValue() {
        TimestampStore store = mock(TimestampStore.class);
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);

        tracker.update("orders", 1000L);
        tracker.update("orders", 2000L);

        verify(store).save("orders", 1000L);
        verify(store).save("orders", 2000L);
    }

    @Test
    void restoreLoadsExistingStoreContents() {
        TimestampStore store = mock(TimestampStore.class);
        when(store.loadAll()).thenReturn(Map.of("orders", 5000L, "payments", 7000L));

        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
        tracker.restore();

        assertThat(tracker.getLastTimestamp("orders")).isEqualTo(5000L);
        assertThat(tracker.getLastTimestamp("payments")).isEqualTo(7000L);
    }

    @Test
    void getAllTimestampsReturnsImmutableCopy() {
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);
        tracker.update("orders", 1000L);
        tracker.update("payments", 2000L);

        Map<String, Long> snapshot = tracker.getAllTimestamps();

        assertThat(snapshot).containsExactlyInAnyOrderEntriesOf(
                Map.of("orders", 1000L, "payments", 2000L));
    }
}
