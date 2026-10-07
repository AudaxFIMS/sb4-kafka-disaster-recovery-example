package dev.semeshin.kafkadr.consumer;

import org.springframework.messaging.Message;

import java.time.Duration;

/**
 * Holds a consumer back while a cluster group it depends on ({@code depends-on}) is down.
 *
 * <p>A handler that forwards into another Kafka cannot do its work while that Kafka has no
 * healthy cluster. Throwing is the wrong signal there: every failed delivery counts against the
 * retry budget, the budget is spent in seconds because the send fails fast, and the container
 * then skips the record. A negative acknowledgment is the right one — the record is sought back
 * and redelivered after a pause, and no delivery attempt is counted. The gate decides when that
 * applies; the consumers carry it out through {@link DependencyNacks}, because only they hold
 * the acknowledgment.
 */
public interface DependencyGate {

    /** No dependencies: never blocks, never reclassifies a failure. */
    DependencyGate NONE = new DependencyGate() {
        @Override
        public String blockingDependency(String clusterName) {
            return null;
        }

        @Override
        public boolean isDependencyFailure(String clusterName, Throwable failure) {
            return false;
        }

        @Override
        public Duration nackInterval() {
            return Duration.ZERO;
        }
    };

    /**
     * The dependency group that has no healthy cluster right now, or null when processing may
     * go ahead. Asked before a delivery reaches the handler.
     *
     * @param clusterName binder id of the cluster the delivery came from
     */
    String blockingDependency(String clusterName);

    /**
     * Whether a handler failure was caused by a dependency being unavailable — either one is
     * down now, or the failure is a {@code ClusterGroupUnavailableException} for one of them.
     * Such a failure is negatively acknowledged instead of propagated.
     */
    boolean isDependencyFailure(String clusterName, Throwable failure);

    /** Pause before a held-back record is redelivered. */
    Duration nackInterval();

    /**
     * Whether a record whose handler failed on a dependency may be held back once more. False once
     * the record has been held back for {@code depends-on-max-hold-ms} while its dependencies
     * were available: the group is up and still does not take it, so holding it longer would
     * stall the consumer for good. The record then takes the ordinary failure path. Time a
     * dependency spends down never counts.
     *
     * @param record the record that would be held back
     */
    default boolean mayHoldBack(String clusterName, Message<?> record) {
        return true;
    }
}
