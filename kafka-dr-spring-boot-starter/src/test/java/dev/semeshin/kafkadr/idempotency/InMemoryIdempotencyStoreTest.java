package dev.semeshin.kafkadr.idempotency;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryIdempotencyStoreTest {

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();

    @Test
    void firstCallReturnsTrue() {
        assertThat(store.tryProcess("orders-consumer", "msg-1")).isTrue();
    }

    @Test
    void duplicateCallReturnsFalse() {
        store.tryProcess("orders-consumer", "msg-1");
        assertThat(store.tryProcess("orders-consumer", "msg-1")).isFalse();
    }

    @Test
    void differentMessagesForSameConsumerAreIndependent() {
        assertThat(store.tryProcess("orders-consumer", "msg-1")).isTrue();
        assertThat(store.tryProcess("orders-consumer", "msg-2")).isTrue();
    }

    @Test
    void sameMessageIdAcrossDifferentConsumersIsNotADuplicate() {
        store.tryProcess("orders-consumer", "shared-id");
        assertThat(store.tryProcess("payments-consumer", "shared-id")).isTrue();
    }

    @Test
    void evictExpiredKeepsRecentEntries() {
        store.tryProcess("c1", "fresh-1");
        store.tryProcess("c1", "fresh-2");

        store.evictExpired();

        assertThat(store.tryProcess("c1", "fresh-1")).isFalse();
        assertThat(store.tryProcess("c1", "fresh-2")).isFalse();
    }

    @Test
    void evictExpiredRemovesEntriesOlderThanTtl() {
        ConcurrentHashMap<String, Instant> entries = stateOf(store);
        entries.put("c1:old-1", Instant.now().minusSeconds(7200));
        entries.put("c1:old-2", Instant.now().minusSeconds(7200));
        entries.put("c1:fresh", Instant.now());

        store.evictExpired();

        assertThat(store.tryProcess("c1", "old-1")).isTrue();
        assertThat(store.tryProcess("c1", "old-2")).isTrue();
        assertThat(store.tryProcess("c1", "fresh")).isFalse();
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Instant> stateOf(InMemoryIdempotencyStore store) {
        return (ConcurrentHashMap<String, Instant>) ReflectionTestUtils.getField(store, "processedIds");
    }
}
