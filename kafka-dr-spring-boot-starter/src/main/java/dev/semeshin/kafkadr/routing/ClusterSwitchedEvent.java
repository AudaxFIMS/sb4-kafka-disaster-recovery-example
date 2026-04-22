package dev.semeshin.kafkadr.routing;

import org.springframework.context.ApplicationEvent;

public class ClusterSwitchedEvent extends ApplicationEvent {

    private final String previousCluster;
    private final String newCluster;

    public ClusterSwitchedEvent(Object source, String previousCluster, String newCluster) {
        super(source);
        this.previousCluster = previousCluster;
        this.newCluster = newCluster;
    }

    public String getPreviousCluster() { return previousCluster; }
    public String getNewCluster() { return newCluster; }
}
