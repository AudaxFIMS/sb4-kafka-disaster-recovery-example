package dev.semeshin.kafkadr.consumer;

import java.util.Map;

/**
 * Persists last processed timestamps.
 * Implement and register as @Component to survive application restarts.
 * When no implementation is provided, timestamps are kept in memory only
 * and lost on restart (seek-by-timestamp falls back to committed offsets).
 *
 * <p>The key is opaque to implementations. The tracker passes
 * {@code <consumer>:topic-partition} — one watermark per consumer and partition. Existing stores
 * keep working unchanged; entries written under an older key format ({@code topic-partition},
 * or a bare topic name before that) are never read again, so the first failover after upgrading
 * falls back to committed offsets once.
 */
public interface TimestampStore {

    void save(String key, long timestamp);

    Long load(String key);

    Map<String, Long> loadAll();
}
