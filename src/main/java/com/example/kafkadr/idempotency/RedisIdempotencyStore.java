package com.example.kafkadr.idempotency;

import com.example.kafkadr.config.KafkaClusterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Profile("redis-idempotency")
public class RedisIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyStore.class);

    private final StringRedisTemplate redis;
    private final String keyPrefix;
    private final Duration ttl;

    public RedisIdempotencyStore(StringRedisTemplate redis,
                                 KafkaClusterProperties properties) {
        this.redis = redis;
        this.keyPrefix = properties.getIdempotency().getKeyPrefix();
        this.ttl = Duration.ofSeconds(properties.getIdempotency().getTtlSeconds());
    }

    @Override
    public boolean tryProcess(String consumerName, String messageId) {
        String key = keyPrefix + ":" + consumerName + ":" + messageId;

        Boolean wasSet = redis.opsForValue().setIfAbsent(key, "1", ttl);

        if (Boolean.TRUE.equals(wasSet)) {
            return true;
        }

        log.debug("Duplicate detected: consumer={}, messageId={}", consumerName, messageId);
        return false;
    }
}
