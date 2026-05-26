package dev.semeshin.kafkadr.routing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterSwitchedEventTest {

    @Test
    void carriesPreviousAndNewClusterNames() {
        Object source = new Object();
        ClusterSwitchedEvent event = new ClusterSwitchedEvent(source, "primary", "secondary");

        assertThat(event.getSource()).isSameAs(source);
        assertThat(event.getPreviousCluster()).isEqualTo("primary");
        assertThat(event.getNewCluster()).isEqualTo("secondary");
    }
}
