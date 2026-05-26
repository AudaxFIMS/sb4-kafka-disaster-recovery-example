package dev.semeshin.kafkadr.config;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupClusterStateTest {

    @Test
    void freshStateIsEmpty() {
        StartupClusterState state = newWithProperty(null);
        assertThat(state.getInitializedClusters()).isEmpty();
        assertThat(state.isInitialized("primary")).isFalse();
    }

    @Test
    void blankPropertyResultsInEmptyState() {
        StartupClusterState state = newWithProperty("   ");
        assertThat(state.getInitializedClusters()).isEmpty();
    }

    @Test
    void initPopulatesFromCommaSeparatedProperty() {
        StartupClusterState state = newWithProperty("primary,secondary,tertiary");
        assertThat(state.getInitializedClusters())
                .containsExactlyInAnyOrder("primary", "secondary", "tertiary");
        assertThat(state.isInitialized("primary")).isTrue();
        assertThat(state.isInitialized("ghost")).isFalse();
    }

    @Test
    void addInitializedClusterTracksAtRuntime() {
        StartupClusterState state = newWithProperty(null);
        state.addInitializedCluster("late-cluster");

        assertThat(state.isInitialized("late-cluster")).isTrue();
    }

    @Test
    void getInitializedClustersReturnsImmutableCopy() {
        StartupClusterState state = newWithProperty("primary");

        assertThatThrownBy(() -> state.getInitializedClusters().add("hack"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static StartupClusterState newWithProperty(String value) {
        StartupClusterState state = new StartupClusterState();
        ReflectionTestUtils.setField(state, "initializedClustersProperty", value);
        ReflectionTestUtils.invokeMethod(state, "init");
        return state;
    }
}
