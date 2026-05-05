package dev.semeshin.kafkadr.routing;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default in-memory store. Restart loses state, so failback-after is best-effort
 * across restarts unless a durable implementation is provided.
 */
public class InMemoryFailoverStateStore implements FailoverStateStore {

    private final AtomicReference<FailoverState> state = new AtomicReference<>();

    @Override
    public void save(FailoverState newState) {
        state.set(newState);
    }

    @Override
    public Optional<FailoverState> load() {
        return Optional.ofNullable(state.get());
    }

    @Override
    public void clear() {
        state.set(null);
    }
}
