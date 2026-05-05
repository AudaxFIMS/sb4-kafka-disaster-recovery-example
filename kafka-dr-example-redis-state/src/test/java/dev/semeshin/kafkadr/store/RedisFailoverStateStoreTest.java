package dev.semeshin.kafkadr.store;

import dev.semeshin.kafkadr.routing.FailoverStateStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit test for the Redis-backed FailoverStateStore.
 *
 * The test uses a mocked StringRedisTemplate with a simple in-memory hash to
 * exercise the save/load/clear contract. It verifies:
 *  - save persists both fields and round-trips through load,
 *  - load returns Optional.empty() when no state has been written,
 *  - clear deletes the persisted hash,
 *  - load tolerates partial/garbage data without throwing.
 */
class RedisFailoverStateStoreTest {

    private static final String KEY = "kafka-dr:failover-state";

    private StringRedisTemplate redis;
    private HashOperations<String, Object, Object> hashOps;
    private Map<String, String> backing;
    private RedisFailoverStateStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        hashOps = mock(HashOperations.class);
        backing = new HashMap<>();

        when(redis.opsForHash()).thenReturn((HashOperations) hashOps);

        doAnswer(inv -> {
            Map<String, String> source = inv.getArgument(1);
            backing.putAll(source);
            return null;
        }).when(hashOps).putAll(eq(KEY), any());

        when(hashOps.entries(eq(KEY))).thenAnswer(inv -> new HashMap<>(backing));

        when(redis.delete(eq(KEY))).thenAnswer(inv -> {
            boolean had = !backing.isEmpty();
            backing.clear();
            return had;
        });

        store = new RedisFailoverStateStore(redis);
    }

    @Test
    void loadReturnsEmptyWhenNothingPersisted() {
        assertThat(store.load()).isEmpty();
    }

    @Test
    void saveThenLoadRoundTrip() {
        Instant at = Instant.parse("2026-05-05T12:34:56Z");
        store.save(new FailoverStateStore.FailoverState("secondary", at));

        Optional<FailoverStateStore.FailoverState> loaded = store.load();

        assertThat(loaded).isPresent();
        assertThat(loaded.get().activeCluster()).isEqualTo("secondary");
        assertThat(loaded.get().failoverAt()).isEqualTo(at);
    }

    @Test
    void clearRemovesPersistedState() {
        store.save(new FailoverStateStore.FailoverState("secondary", Instant.now()));
        store.clear();

        assertThat(store.load()).isEmpty();
        verify(redis).delete(KEY);
    }

    @Test
    void loadIgnoresIncompleteHash() {
        backing.put("activeCluster", "secondary");
        // failoverAt is missing on purpose

        assertThat(store.load()).isEmpty();
    }

    @Test
    void loadIgnoresUnparseableInstant() {
        backing.put("activeCluster", "secondary");
        backing.put("failoverAt", "not-a-real-instant");

        assertThat(store.load()).isEmpty();
    }
}
