package dev.semeshin.kafkadr.store;

import dev.semeshin.kafkadr.routing.FailoverStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Redis-backed FailoverStateStore. Persists the active cluster and the
 * timestamp of the failover so that failback-after is honored across
 * application restarts.
 */
@ConditionalOnClass(StringRedisTemplate.class)
@Component
public class RedisFailoverStateStore implements FailoverStateStore {

    private static final Logger log = LoggerFactory.getLogger(RedisFailoverStateStore.class);

    private static final String KEY = "kafka-dr:failover-state";
    private static final String FIELD_CLUSTER = "activeCluster";
    private static final String FIELD_FAILOVER_AT = "failoverAt";

    private final StringRedisTemplate redis;

    public RedisFailoverStateStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void save(FailoverState state) {
        Map<String, String> fields = Map.of(
                FIELD_CLUSTER, state.activeCluster(),
                FIELD_FAILOVER_AT, state.failoverAt().toString()
        );
        redis.<String, String>opsForHash().putAll(KEY, fields);
        log.info("Persisted failover state: cluster={}, at={}", state.activeCluster(), state.failoverAt());
    }

    @Override
    public Optional<FailoverState> load() {
        Map<Object, Object> entries = redis.opsForHash().entries(KEY);
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        Object cluster = entries.get(FIELD_CLUSTER);
        Object at = entries.get(FIELD_FAILOVER_AT);
        if (cluster == null || at == null) {
            log.warn("Incomplete failover state in Redis — ignoring: {}", entries);
            return Optional.empty();
        }
        try {
            return Optional.of(new FailoverState(cluster.toString(), Instant.parse(at.toString())));
        } catch (Exception e) {
            log.warn("Failed to parse persisted failover state {} — ignoring", entries, e);
            return Optional.empty();
        }
    }

    @Override
    public void clear() {
        Boolean deleted = redis.delete(KEY);
        if (Boolean.TRUE.equals(deleted)) {
            log.info("Cleared persisted failover state");
        }
    }
}
