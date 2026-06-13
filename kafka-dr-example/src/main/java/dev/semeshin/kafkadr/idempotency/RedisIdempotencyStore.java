package dev.semeshin.kafkadr.idempotency;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Redis-backed idempotency store. Replaces the default InMemoryIdempotencyStore
 * when registered as a @Component bean. Suitable for multi-instance deployments.
 * Only created when kafka-dr is active and idempotency is not disabled.
 */
@Component
@ConditionalOnProperty(name = "kafka-dr.idempotency.enabled", havingValue = "true", matchIfMissing = true)
public class RedisIdempotencyStore implements IdempotencyStore {
    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyStore.class);

    private final StringRedisTemplate redis;
    private final String keyPrefix;
    private final String keyHeader;
    private final Duration ttl;

    public RedisIdempotencyStore(StringRedisTemplate redis,
                                 KafkaClusterProperties properties) {
        this.redis = redis;
        this.keyPrefix = properties.getIdempotency().getKeyPrefix();
        this.keyHeader = properties.getIdempotency().getKeyHeader();
        this.ttl = Duration.ofSeconds(properties.getIdempotency().getTtlSeconds());
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
        String messageKey = extractKey(message);
        if (messageKey == null) {
            log.warn("[{}][{}] Message without key, processing without idempotency check", clusterName, consumerName);
            return true;
        }

        String compositeKey = keyPrefix + ":" + consumerName + ":" + messageKey;

        Boolean wasSet = redis.opsForValue().setIfAbsent(compositeKey, "1", ttl);

        if (Boolean.TRUE.equals(wasSet)) {
	        log.debug("[{}][{}] Message with idempotency key accepted: {}", clusterName, consumerName, compositeKey);
            return true;
        }

	    log.debug("[{}][{}] Duplicate message with idempotency key detected: {}", clusterName, consumerName, compositeKey);

	    return false;
    }
}
