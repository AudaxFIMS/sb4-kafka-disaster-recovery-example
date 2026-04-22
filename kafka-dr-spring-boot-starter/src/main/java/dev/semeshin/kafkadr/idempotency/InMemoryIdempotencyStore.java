package dev.semeshin.kafkadr.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory fallback. Created automatically when no other IdempotencyStore bean is registered.
 * Not suitable for multi-instance deployments.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@ConditionalOnMissingBean(IdempotencyStore.class)
@Component
public class InMemoryIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);
    private static final long TTL_SECONDS = 3600;

    private final ConcurrentHashMap<String, Instant> processedIds = new ConcurrentHashMap<>();

    @Override
    public boolean tryProcess(String consumerName, String messageId) {
        String compositeKey = consumerName + ":" + messageId;
        Instant previous = processedIds.putIfAbsent(compositeKey, Instant.now());
        if (previous != null) {
            log.debug("Duplicate detected: {}", compositeKey);
            return false;
        }
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
