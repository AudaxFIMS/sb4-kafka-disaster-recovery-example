package dev.semeshin.kafkadr.consumer;

import java.util.Map;

/**
 * Persists last processed timestamps.
 * Implement and register as @Component to survive application restarts.
 * When no implementation is provided, timestamps are kept in memory only
 * and lost on restart (seek-by-timestamp falls back to committed offsets).
 *
 * <p>The key is opaque to implementations: since 2026-08 the tracker passes
 * {@code topic-partition} rather than a bare topic name. Existing stores keep
 * working unchanged; entries written under the old key format are simply never
 * read again, so the first start after upgrading falls back to committed offsets
 * once.
 */
public interface TimestampStore {

    void save(String key, long timestamp);

    Long load(String key);

    Map<String, Long> loadAll();
}
