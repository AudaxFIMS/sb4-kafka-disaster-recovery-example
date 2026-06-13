package dev.semeshin.kafkadr.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
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
    private static final long TTL_SECONDS = 3600;

    private final ConcurrentHashMap<String, Instant> processedIds = new ConcurrentHashMap<>();
    private final String keyHeader;

    public InMemoryIdempotencyStore() {
        this(null);
    }

    /**
     * @param keyHeader optional custom header to use as deduplication key
     *                  instead of the Kafka record key (kafka-dr.idempotency.key-header)
     */
    public InMemoryIdempotencyStore(String keyHeader) {
        this.keyHeader = keyHeader;
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
        Instant previous = processedIds.putIfAbsent(compositeKey, Instant.now());
        if (previous != null) {
            log.debug("[{}][{}] Duplicate message with idempotency key detected: {}", clusterName, consumerName, compositeKey);
            return false;
        }
	    log.debug("[{}][{}] Message with idempotency key accepted: {}", clusterName, consumerName, compositeKey);

        return true;
    }

    @Scheduled(fixedRate = 300_000)
    public void evictExpired() {
        Instant cutoff = Instant.now().minusSeconds(TTL_SECONDS);
        int before = processedIds.size();
        processedIds.entrySet().removeIf(e -> e.getValue().isBefore(cutoff));
        int evicted = before - processedIds.size();
        if (evicted > 0) {
            log.info("Evicted {} expired idempotency entries, {} remaining", evicted, processedIds.size());
        }
    }
}
