package dev.semeshin.kafkadr.consumer;

import java.util.Map;

/**
 * Persists last processed timestamps per topic.
 * Implement and register as @Component to survive application restarts.
 * When no implementation is provided, timestamps are kept in memory only
 * and lost on restart (seek-by-timestamp falls back to committed offsets).
 */
public interface TimestampStore {

    void save(String topic, long timestamp);

    Long load(String topic);

    Map<String, Long> loadAll();
}
