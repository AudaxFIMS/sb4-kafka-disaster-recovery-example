package dev.semeshin.kafkadr.store;

import dev.semeshin.kafkadr.consumer.TimestampStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis-backed timestamp store. Persists last processed timestamps
 * so seek-by-timestamp works correctly after application restart.
 */
@ConditionalOnClass(StringRedisTemplate.class)
@Component
public class RedisTimestampStore implements TimestampStore {

    private static final String KEY = "kafka-dr:last-timestamps";

    private final StringRedisTemplate redis;

    public RedisTimestampStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void save(String topic, long timestamp) {
        redis.opsForHash().put(KEY, topic, String.valueOf(timestamp));
    }

    @Override
    public Long load(String topic) {
        Object value = redis.opsForHash().get(KEY, topic);
        return value != null ? Long.parseLong(value.toString()) : null;
    }

    @Override
    public Map<String, Long> loadAll() {
        Map<Object, Object> entries = redis.opsForHash().entries(KEY);
        Map<String, Long> result = new HashMap<>();
        entries.forEach((k, v) -> result.put(k.toString(), Long.parseLong(v.toString())));
        return result;
    }
}
