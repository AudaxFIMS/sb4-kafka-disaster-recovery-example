package dev.semeshin.kafkadr.routing;

import org.springframework.context.ApplicationEvent;

/**
 * A cluster group lost its last healthy cluster, or regained one.
 *
 * <p>Distinct from {@link ClusterSwitchedEvent}: a group whose every cluster is down has
 * nowhere to switch to, so no switch is published — yet that is exactly the moment anything
 * depending on the group has to stop feeding it. Published only on the transition, never
 * repeated while the state holds.
 */
public class ClusterGroupAvailabilityEvent extends ApplicationEvent {

    private final String group;
    private final boolean available;

    public ClusterGroupAvailabilityEvent(Object source, String group, boolean available) {
        super(source);
        this.group = group;
        this.available = available;
    }

    public String getGroup() { return group; }

    /** True when at least one cluster of the group is healthy. */
    public boolean isAvailable() { return available; }
}
