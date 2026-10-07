package dev.semeshin.kafkadr.routing;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import org.springframework.context.ApplicationEvent;

/**
 * The active cluster of one cluster group changed. Other groups are unaffected — each fails
 * over on its own.
 */
public class ClusterSwitchedEvent extends ApplicationEvent {

    private final String group;
    private final String previousCluster;
    private final String newCluster;

    /** A switch in the {@code default} group — the {@code kafka-dr.clusters} form. */
    public ClusterSwitchedEvent(Object source, String previousCluster, String newCluster) {
        this(source, KafkaClusterProperties.DEFAULT_CLUSTER_GROUP, previousCluster, newCluster);
    }

    /**
     * @param previousCluster binder id of the cluster the group leaves
     * @param newCluster      binder id of the cluster the group moves to
     */
    public ClusterSwitchedEvent(Object source, String group, String previousCluster, String newCluster) {
        super(source);
        this.group = group;
        this.previousCluster = previousCluster;
        this.newCluster = newCluster;
    }

    public String getGroup() { return group; }
    public String getPreviousCluster() { return previousCluster; }
    public String getNewCluster() { return newCluster; }
}
