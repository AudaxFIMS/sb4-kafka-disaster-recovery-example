package dev.semeshin.kafkadr.routing;

import java.time.Instant;
import java.util.Optional;

/**
 * Persists which cluster the app is pinned to after a failover and when the
 * failover happened. Used to honor failback-after across application restarts.
 * <p>
 * Default implementation is in-memory (no cross-restart durability). Provide a
 * custom bean (e.g. backed by Redis, Consul, a Kafka compacted topic, etc.) to
 * survive restarts.
 *
 * <p>Every cluster group fails over on its own, so the state is kept per group. The
 * per-group methods default to the single-state ones, which is exactly right while one group
 * is configured — every existing implementation keeps working unchanged. With several groups
 * those defaults would let the groups overwrite each other's state, so the starter refuses to
 * start unless the store says, through {@link #supportsGroups()}, that it keeps them apart.
 */
public interface FailoverStateStore {

    /**
     * Records that the app failed over to {@code activeCluster} at {@code failoverAt}.
     */
    void save(FailoverState state);

    /**
     * Returns the persisted failover state, if any.
     */
    Optional<FailoverState> load();

    /**
     * Clears the persisted state (called on a successful failback).
     */
    void clear();

    /**
     * Records a failover of one cluster group.
     *
     * @param group cluster group name; {@code default} for the {@code kafka-dr.clusters} form
     */
    default void save(String group, FailoverState state) {
        save(state);
    }

    /**
     * Returns the persisted failover state of one cluster group, if any.
     */
    default Optional<FailoverState> load(String group) {
        return load();
    }

    /**
     * Clears the persisted state of one cluster group (called on its failback).
     */
    default void clear(String group) {
        clear();
    }

    /**
     * Whether this store keeps a separate state per cluster group — i.e. implements the three
     * per-group methods with a key per group. Required when several groups are configured.
     * An explicit declaration rather than a guess from the class's methods, which proxies and
     * wrappers would make unreliable.
     */
    default boolean supportsGroups() {
        return false;
    }

    /**
     * @param activeCluster binder id of the cluster the group failed over to — the plain cluster
     *                      name in the {@code default} group, {@code <group>-<cluster>} elsewhere
     */
    record FailoverState(String activeCluster, Instant failoverAt) {}
}
