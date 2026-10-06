package dev.semeshin.kafkadr.idempotency;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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

        String compositeKey = compositeKey(consumerName, messageKey);

        Boolean wasSet = redis.opsForValue().setIfAbsent(compositeKey, "1", ttl);

        if (Boolean.TRUE.equals(wasSet)) {
	        log.debug("[{}][{}] Message with idempotency key accepted: {}", clusterName, consumerName, compositeKey);
            return true;
        }

	    log.debug("[{}][{}] Duplicate message with idempotency key detected: {}", clusterName, consumerName, compositeKey);

	    return false;
    }

    /**
     * Pipelined batch check: one round-trip for the whole batch instead of one per record.
     * With max.poll.records=500 that is the difference between 500 sequential network
     * calls and a single one, which is where most of the value of batching sits here.
     *
     * <p>Redis still executes the SETNXs in order, so two records sharing a key inside
     * one batch behave exactly as they would one at a time: the second is a duplicate.
     */
    @Override
    public List<Message<?>> filterProcessable(String clusterName, String consumerName,
                                              List<Message<?>> messages) {
        List<String> keys = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            keys.add(extractKey(message));
        }

        List<Object> results = redis.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection stringConn = (StringRedisConnection) connection;
            for (String key : keys) {
                if (key != null) {
                    stringConn.set(compositeKey(consumerName, key), "1",
                            Expiration.from(ttl), SetOption.ifAbsent());
                }
            }
            return null;
        });

        List<Message<?>> accepted = new ArrayList<>(messages.size());
        int resultIndex = 0;
        for (int i = 0; i < messages.size(); i++) {
            if (keys.get(i) == null) {
                // No key means no deduplication, exactly as in tryProcess.
                log.warn("[{}][{}] Message without key, processing without idempotency check",
                        clusterName, consumerName);
                accepted.add(messages.get(i));
                continue;
            }
            Object result = resultIndex < results.size() ? results.get(resultIndex++) : null;
            if (Boolean.TRUE.equals(result)) {
                accepted.add(messages.get(i));
            }
        }

        log.debug("[{}][{}] Batch of {}: {} accepted, {} duplicates",
                clusterName, consumerName, messages.size(), accepted.size(),
                messages.size() - accepted.size());
        return accepted;
    }

    @Override
    public void rollback(String clusterName, String consumerName, List<Message<?>> messages) {
        List<String> keys = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            String messageKey = extractKey(message);
            if (messageKey != null) {
                keys.add(compositeKey(consumerName, messageKey));
            }
        }
        if (keys.isEmpty()) {
            return;
        }
        Long removed = redis.delete(keys);
        log.debug("[{}][{}] Rolled back {} of {} idempotency keys",
                clusterName, consumerName, removed, keys.size());
    }

    private String compositeKey(String consumerName, String messageKey) {
        return keyPrefix + ":" + consumerName + ":" + messageKey;
    }
}
