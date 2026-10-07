package dev.semeshin.kafkadr.store;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
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
 *
 * <p>One hash per cluster group, since each group fails over on its own. The
 * {@code default} group — the {@code kafka-dr.clusters} form — keeps the original key, so
 * state written before cluster groups existed is still found after the upgrade.
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

    static String keyOf(String group) {
        return KafkaClusterProperties.DEFAULT_CLUSTER_GROUP.equals(group) ? KEY : KEY + ":" + group;
    }

    @Override
    public void save(FailoverState state) {
        save(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP, state);
    }

    @Override
    public Optional<FailoverState> load() {
        return load(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP);
    }

    @Override
    public void clear() {
        clear(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP);
    }

    @Override
    public boolean supportsGroups() {
        return true;
    }

    @Override
    public void save(String group, FailoverState state) {
        Map<String, String> fields = Map.of(
                FIELD_CLUSTER, state.activeCluster(),
                FIELD_FAILOVER_AT, state.failoverAt().toString()
        );
        redis.<String, String>opsForHash().putAll(keyOf(group), fields);
        log.info("Persisted failover state of group {}: cluster={}, at={}", group, state.activeCluster(),
                state.failoverAt());
    }

    @Override
    public Optional<FailoverState> load(String group) {
        Map<Object, Object> entries = redis.opsForHash().entries(keyOf(group));
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        Object cluster = entries.get(FIELD_CLUSTER);
        Object at = entries.get(FIELD_FAILOVER_AT);
        if (cluster == null || at == null) {
            log.warn("Incomplete failover state of group {} in Redis — ignoring: {}", group, entries);
            return Optional.empty();
        }
        try {
            return Optional.of(new FailoverState(cluster.toString(), Instant.parse(at.toString())));
        } catch (Exception e) {
            log.warn("Failed to parse persisted failover state of group {} {} — ignoring", group, entries, e);
            return Optional.empty();
        }
    }

    @Override
    public void clear(String group) {
        Boolean deleted = redis.delete(keyOf(group));
        if (Boolean.TRUE.equals(deleted)) {
            log.info("Cleared persisted failover state of group {}", group);
        }
    }
}
