package dev.semeshin.kafkadr.consumer;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the latest Kafka record timestamp per topic partition.
 * Used by failover logic to seek consumers on the new cluster
 * to the offset matching the last processed timestamp.
 *
 * <p>Entries are keyed by {@code topic-partition}, matching what
 * {@link TimestampSeekRebalanceListener} looks up for each assigned partition.
 * A per-topic watermark would be the maximum across partitions, which seeks
 * lagging partitions past records that were never processed.
 *
 * <p>If a TimestampStore bean is available, timestamps are persisted
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

    /**
     * Records the timestamp of a processed record, keeping the highest value seen
     * for that partition.
     *
     * <p>The merge is atomic on purpose: with consumer {@code concurrency > 1} the
     * same tracker is updated from several container threads, and a read-compare-write
     * would let a lower timestamp overwrite a higher one — moving the watermark
     * backwards and seeking earlier than necessary after a failover.
     */
    public void update(String topic, int partition, long timestamp) {
        String key = key(topic, partition);
        long merged = lastTimestamps.merge(key, timestamp, Math::max);
        // Only persist when this call actually advanced the watermark.
        if (store != null && merged == timestamp) {
            store.save(key, timestamp);
        }
    }

    /**
     * Advances the watermark from a consumed record's Kafka headers.
     *
     * @return false when the record carries no topic/partition/timestamp, so the caller
     *         can report that the watermark could not be placed
     */
    public boolean advance(Message<?> record) {
        Long timestamp = record.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP, Long.class);
        String topic = record.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, String.class);
        Integer partition = record.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION, Integer.class);

        if (timestamp == null || topic == null || partition == null) {
            return false;
        }
        update(topic, partition, timestamp);
        return true;
    }

    public Long getLastTimestamp(String topic, int partition) {
        return lastTimestamps.get(key(topic, partition));
    }

    private static String key(String topic, int partition) {
        return topic + "-" + partition;
    }

    public Map<String, Long> getAllTimestamps() {
        return Map.copyOf(lastTimestamps);
    }
}
