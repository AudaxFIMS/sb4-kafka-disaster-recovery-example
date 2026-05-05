package dev.semeshin.kafkadr.routing;

import java.time.Instant;
import java.util.Optional;

/**
 * Persists which cluster the app is pinned to after a failover and when the
 * failover happened. Used to honor failback-after across application restarts.
 *
 * Default implementation is in-memory (no cross-restart durability). Provide a
 * custom bean (e.g. backed by Redis, Consul, a Kafka compacted topic, etc.) to
 * survive restarts.
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

    record FailoverState(String activeCluster, Instant failoverAt) {}
}
