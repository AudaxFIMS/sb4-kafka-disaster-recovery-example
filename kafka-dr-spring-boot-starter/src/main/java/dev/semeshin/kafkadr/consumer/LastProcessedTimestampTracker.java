package dev.semeshin.kafkadr.consumer;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the latest Kafka record timestamp per topic.
 * Used by failover logic to seek consumers on the new cluster
 * to the offset matching the last processed timestamp.
 *
 * If a TimestampStore bean is available, timestamps are persisted
 * and restored on restart. Otherwise, they are in-memory only.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class LastProcessedTimestampTracker {

    private static final Logger log = LoggerFactory.getLogger(LastProcessedTimestampTracker.class);

    private final Map<String, Long> lastTimestamps = new ConcurrentHashMap<>();
    private final TimestampStore store;

    public LastProcessedTimestampTracker(@Nullable TimestampStore store) {
        this.store = store;
    }

    @PostConstruct
    void restore() {
        if (store == null) return;
        Map<String, Long> restored = store.loadAll();
        if (!restored.isEmpty()) {
            lastTimestamps.putAll(restored);
            log.info("Restored {} topic timestamps from store", restored.size());
        }
    }

    public void update(String topic, long timestamp) {
        Long prev = lastTimestamps.get(topic);
        if (prev == null || timestamp > prev) {
            lastTimestamps.put(topic, timestamp);
            if (store != null) {
                store.save(topic, timestamp);
            }
        }
    }

    public Long getLastTimestamp(String topic) {
        return lastTimestamps.get(topic);
    }

    public Map<String, Long> getAllTimestamps() {
        return Map.copyOf(lastTimestamps);
    }
}
