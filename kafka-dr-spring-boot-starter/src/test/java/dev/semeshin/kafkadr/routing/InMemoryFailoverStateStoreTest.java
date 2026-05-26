package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.routing.FailoverStateStore.FailoverState;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryFailoverStateStoreTest {

    private final InMemoryFailoverStateStore store = new InMemoryFailoverStateStore();

    @Test
    void loadOnFreshStoreReturnsEmpty() {
        assertThat(store.load()).isEmpty();
    }

    @Test
    void saveAndLoadRoundTrip() {
        Instant when = Instant.parse("2025-01-15T14:00:00Z");
        store.save(new FailoverState("secondary", when));

        assertThat(store.load()).contains(new FailoverState("secondary", when));
    }

    @Test
    void saveOverwritesPreviousState() {
        store.save(new FailoverState("secondary", Instant.parse("2025-01-15T14:00:00Z")));
        store.save(new FailoverState("tertiary", Instant.parse("2025-01-15T16:00:00Z")));

        assertThat(store.load()).contains(
                new FailoverState("tertiary", Instant.parse("2025-01-15T16:00:00Z")));
    }

    @Test
    void clearRemovesState() {
        store.save(new FailoverState("secondary", Instant.now()));
        store.clear();

        assertThat(store.load()).isEmpty();
    }
}
