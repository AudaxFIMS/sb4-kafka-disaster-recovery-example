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
import static org.mockito.Mockito.*;

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

	// --- per consumer --------------------------------------------------------------------

	@Test
	void unscopedTrackerKeepsTheHistoricalKeys() {
		TimestampStore store = mock(TimestampStore.class);
		LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);

		assertThat(tracker.forConsumer(null)).isSameAs(tracker);
		assertThat(tracker.consumer()).isNull();
		tracker.update("orders", 0, 1000L);

		verify(store).save("orders-0", 1000L);
	}

	@Test
	void laggingConsumerDoesNotInheritTheWatermarkOfOneAhead() {
		TimestampStore store = mock(TimestampStore.class);
		LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
		LastProcessedTimestampTracker fast = tracker.forConsumer("orders-main");
		LastProcessedTimestampTracker slow = tracker.forConsumer("orders-audit");

		fast.update("orders", 0, 5000L);
		slow.update("orders", 0, 1000L);

		// A shared watermark would be 5000 for both, and after a failover the audit consumer
		// would seek past the records between 1000 and 5000 it never processed.
		assertThat(slow.getLastTimestamp("orders", 0)).isEqualTo(1000L);
		assertThat(fast.getLastTimestamp("orders", 0)).isEqualTo(5000L);
		assertThat(tracker.getLastTimestamp("orders", 0)).isNull();
		verify(store).save("orders-main:orders-0", 5000L);
		verify(store).save("orders-audit:orders-0", 1000L);
		assertThat(slow.consumer()).isEqualTo("orders-audit");
		assertThat(tracker.forConsumer("orders-audit")).isSameAs(slow);
		assertThat(slow.forConsumer("orders-main")).isSameAs(fast);
	}

	@Test
	void restoreBringsBackTheWatermarksOfEveryConsumer() {
		TimestampStore store = mock(TimestampStore.class);
		when(store.loadAll()).thenReturn(Map.of("orders-0", 1000L, "orders-main:orders-0", 2000L));
		LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);

		tracker.restore();

		assertThat(tracker.getLastTimestamp("orders", 0)).isEqualTo(1000L);
		assertThat(tracker.forConsumer("orders-main").getLastTimestamp("orders", 0)).isEqualTo(2000L);
		assertThat(tracker.getAllTimestamps()).containsOnlyKeys("orders-0", "orders-main:orders-0");
	}

	@Test
	void watermarkInThePreviousKeyFormatIsNeverRead() {
		TimestampStore store = mock(TimestampStore.class);
		// Written before watermarks were scoped per consumer — by whichever consumer read the topic.
		when(store.loadAll()).thenReturn(Map.of("orders-0", 1000L, "orders-3", 1000L));
		LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(store);
		tracker.restore();

		// Neither a consumer that existed then nor one added later seeks by another's position:
		// the first failover after upgrading falls back to committed offsets once.
		assertThat(tracker.forConsumer("orders-main").getLastTimestamp("orders", 0)).isNull();
		assertThat(tracker.forConsumer("orders-audit").getLastTimestamp("orders", 3)).isNull();
		verify(store, never()).save(anyString(), anyLong());
	}
}
