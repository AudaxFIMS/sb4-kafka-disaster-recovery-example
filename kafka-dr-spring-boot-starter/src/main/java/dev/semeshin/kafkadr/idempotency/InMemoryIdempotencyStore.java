package dev.semeshin.kafkadr.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory fallback with default key-based deduplication (Kafka record key
 * or the configured kafka-dr.idempotency.key-header). Registered as a @Bean
 * in KafkaDrAutoConfiguration with @ConditionalOnMissingBean so that any
 * custom IdempotencyStore replaces it.
 * Not suitable for multi-instance deployments.
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);
    /** Default of {@code kafka-dr.idempotency.ttl-seconds}. */
    public static final long DEFAULT_TTL_SECONDS = 3600;

    private final ConcurrentHashMap<String, Instant> processedIds = new ConcurrentHashMap<>();
    private final String keyHeader;
    private final long ttlSeconds;

    public InMemoryIdempotencyStore() {
        this(null);
    }

    /**
     * @param keyHeader optional custom header to use as deduplication key
     *                  instead of the Kafka record key (kafka-dr.idempotency.key-header)
     */
    public InMemoryIdempotencyStore(String keyHeader) {
        this(keyHeader, DEFAULT_TTL_SECONDS);
    }

    /**
     * @param keyHeader  optional custom header to use as deduplication key
     * @param ttlSeconds how long a message stays marked as processed
     *                   (kafka-dr.idempotency.ttl-seconds)
     */
    public InMemoryIdempotencyStore(String keyHeader, long ttlSeconds) {
        if (ttlSeconds <= 0) {
            throw new IllegalArgumentException(
                    "kafka-dr.idempotency.ttl-seconds must be positive, was " + ttlSeconds);
        }
        this.keyHeader = keyHeader;
        this.ttlSeconds = ttlSeconds;
    }

    /**
     * Default key-based extraction honoring kafka-dr.idempotency.key-header.
     * Override to derive the key from any headers or payload data.
     */
    @Override
    public String extractKey(Message<?> message) {
        return IdempotencyStore.kafkaKey(message, keyHeader);
    }

    @Override
    public boolean tryProcess(String clusterName, String consumerName, Message<?> message) {
        String key = extractKey(message);
        if (key == null) {
            log.warn("[{}][{}] Message without key, processing without idempotency check", clusterName, consumerName);
            return true;
        }

        String compositeKey = consumerName + ":" + key;
        Instant now = Instant.now();
        Instant cutoff = now.minusSeconds(ttlSeconds);
        // An expired mark counts as absent even before evictExpired gets to it, so a mark
        // lives exactly ttl-seconds, not up to ttl plus the eviction interval.
        boolean[] accepted = {false};
        processedIds.compute(compositeKey, (k, previous) -> {
            if (previous == null || previous.isBefore(cutoff)) {
                accepted[0] = true;
                return now;
            }
            return previous;
        });
        if (!accepted[0]) {
            log.debug("[{}][{}] Duplicate message with idempotency key detected: {}", clusterName, consumerName, compositeKey);
            return false;
        }
	    log.debug("[{}][{}] Message with idempotency key accepted: {}", clusterName, consumerName, compositeKey);

        return true;
    }

    @Override
    public void rollback(String clusterName, String consumerName, List<Message<?>> messages) {
        for (Message<?> message : messages) {
            String key = extractKey(message);
            if (key == null) continue;
            String compositeKey = consumerName + ":" + key;
            if (processedIds.remove(compositeKey) != null) {
                log.debug("[{}][{}] Rolled back idempotency key: {}", clusterName, consumerName, compositeKey);
            }
        }
    }

    @Scheduled(fixedRate = 300_000)
    public void evictExpired() {
        Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
        int before = processedIds.size();
        processedIds.entrySet().removeIf(e -> e.getValue().isBefore(cutoff));
        int evicted = before - processedIds.size();
        if (evicted > 0) {
            log.info("Evicted {} expired idempotency entries, {} remaining", evicted, processedIds.size());
        }
    }
}
