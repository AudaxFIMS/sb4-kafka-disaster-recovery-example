package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default in-memory store. Restart loses state, so failback-after is best-effort
 * across restarts unless a durable implementation is provided.
 *
 * <p>Keeps one state per cluster group; the single-state methods address the
 * {@code default} group.
 */
public class InMemoryFailoverStateStore implements FailoverStateStore {

    private final Map<String, FailoverState> states = new ConcurrentHashMap<>();

    @Override
    public void save(FailoverState newState) {
        save(KafkaClusterProperties.DEFAULT_CLUSTER_GROUP, newState);
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
    public void save(String group, FailoverState newState) {
        states.put(group, newState);
    }

    @Override
    public Optional<FailoverState> load(String group) {
        return Optional.ofNullable(states.get(group));
    }

    @Override
    public void clear(String group) {
        states.remove(group);
    }
}
