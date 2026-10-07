package dev.semeshin.kafkadr.consumer;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks the latest Kafka record timestamp per topic partition.
 * Used by failover logic to seek consumers on the new cluster
 * to the offset matching the last processed timestamp.
 *
 * <p>Entries are keyed by {@code <consumer>:topic-partition}, matching what
 * {@link TimestampSeekRebalanceListener} looks up for each assigned partition.
 * A per-topic watermark would be the maximum across partitions, which seeks
 * lagging partitions past records that were never processed.
 *
 * <p>For the same reason watermarks belong to one consumer. Two consumers reading the same
 * topic progress independently; a shared watermark would be the maximum of the two, and after
 * a failover the one that lags would seek past records it never processed. Each consumer works
 * through {@link #forConsumer(String)}, whose keys carry a {@code <consumer>:} prefix. Consumer
 * names are unique application-wide and a consumer belongs to one cluster group, so the same
 * topic name in two groups — two different Kafkas — never shares a watermark either. All views
 * share one map and one store.
 *
 * <p>If a TimestampStore bean is available, timestamps are persisted
 * and restored on restart. Otherwise, they are in-memory only.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class LastProcessedTimestampTracker {

    private static final Logger log = LoggerFactory.getLogger(LastProcessedTimestampTracker.class);

    private final Map<String, Long> lastTimestamps;
    private final TimestampStore store;
    /** Consumer whose watermarks this view holds; null for the unscoped tracker. */
    private final String consumer;
    private final String keyPrefix;
    /** Every consumer's view, shared by all of them. */
    private final Map<String, LastProcessedTimestampTracker> views;
    private final AtomicBoolean unscopedWarned = new AtomicBoolean();

    @Autowired
    public LastProcessedTimestampTracker(@Nullable TimestampStore store) {
        this(store, new ConcurrentHashMap<>(), null, new ConcurrentHashMap<>());
    }

    private LastProcessedTimestampTracker(TimestampStore store, Map<String, Long> lastTimestamps, String consumer,
                                          Map<String, LastProcessedTimestampTracker> views) {
        this.store = store;
        this.lastTimestamps = lastTimestamps;
        this.consumer = consumer;
        this.keyPrefix = consumer == null ? "" : consumer + ":";
        this.views = views;
    }

    /**
     * The watermarks of one consumer — what its handler path advances and what the seek after
     * its group's failover reads. Watermarks are read and written only through such a view: the
     * bean itself is unscoped, and what it records no seek ever reads.
     */
    public LastProcessedTimestampTracker forConsumer(String consumerName) {
        if (consumerName == null) {
            return this;
        }
        return views.computeIfAbsent(consumerName,
                c -> new LastProcessedTimestampTracker(store, lastTimestamps, c, views));
    }

    /** Consumer whose watermarks this tracker reads and writes; null when unscoped. */
    public String consumer() {
        return consumer;
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
        warnIfUnscoped();
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

    /**
     * The watermark of this consumer for the partition, or null when it has none yet. Entries in
     * an older key format — {@code topic-partition}, written before watermarks were scoped per
     * consumer — are never read: they were the position of whichever consumer wrote them.
     */
    public Long getLastTimestamp(String topic, int partition) {
        return lastTimestamps.get(key(topic, partition));
    }

    /**
     * Recording through the bean itself used to move the failover seek; since watermarks are kept
     * per consumer it records under a key no seek reads. Said once, loudly, rather than silently.
     */
    private void warnIfUnscoped() {
        if (consumer == null && unscopedWarned.compareAndSet(false, true)) {
            log.warn("A watermark was recorded on the unscoped LastProcessedTimestampTracker. No seek reads it: "
                    + "watermarks are kept per consumer — record through tracker.forConsumer(consumerName)");
        }
    }

    private String key(String topic, int partition) {
        return keyPrefix + topic + "-" + partition;
    }

    /** Every watermark of every consumer, under its store key. */
    public Map<String, Long> getAllTimestamps() {
        return Map.copyOf(lastTimestamps);
    }
}
