package dev.semeshin.kafkadr.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.time.Duration;

/**
 * Negative acknowledgments on behalf of a {@link DependencyGate}. A nack seeks the records back
 * and redelivers them after the pause without counting a delivery attempt, which is what keeps
 * a record waiting for its dependency from being skipped once the retries run out.
 */
final class DependencyNacks {

    private static final Logger log = LoggerFactory.getLogger(DependencyNacks.class);

    private DependencyNacks() {
    }

    /**
     * The gate before the handler, for every consumer path: if a dependency is down right now the
     * whole delivery is held back and true returned — the caller must not process it.
     */
    static boolean heldBack(Message<?> delivery, boolean batch, DependencyGate gate,
                            String clusterName, String consumerName) {
        String blocking = gate.blockingDependency(clusterName);
        if (blocking == null) {
            return false;
        }
        holdBack(delivery, batch, 0, gate, blocking, null, clusterName, consumerName);
        return true;
    }

    /**
     * Holds a delivery back from record {@code from} on, for every consumer path the same way:
     * one warning, one nack, and — when there is no acknowledgment to nack with — the failure
     * that caused it, or an exception saying why the record could not be held back.
     *
     * <p>For a batch the records before {@code from} are committed by the same nack, which is how a
     * batch stopped part-way keeps its processed prefix. Nothing else is committed, and no
     * delivery attempt is counted.
     *
     * @param blocking the unavailable dependency group, or null when a handler failure was
     *                 recognized as the dependency being down
     * @param failure  the handler failure being reclassified, or null when the gate stopped the
     *                 delivery before the handler
     */
    static void holdBack(Message<?> delivery, boolean batch, int from, DependencyGate gate, String blocking,
                         RuntimeException failure, String clusterName, String consumerName) {
        Duration interval = gate.nackInterval();
        String dependency = blocking == null ? "A dependency group" : "Dependency group '" + blocking + "'";
        log.warn("[{}][{}] {} unavailable — {} held back from record {}, redelivery in {} ms",
                clusterName, consumerName, dependency, batch ? "batch" : "record", from, interval.toMillis());
        if (nack(delivery, batch, from, interval)) {
            return;
        }
        if (failure != null) {
            throw failure;
        }
        throw new IllegalStateException(("[%s][%s] %s unavailable and the %s cannot be held back without a manual "
                + "acknowledgment").formatted(clusterName, consumerName, dependency, batch ? "batch" : "record"));
    }

    private static boolean nack(Message<?> delivery, boolean batch, int index, Duration interval) {
        Acknowledgment ack = delivery.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            log.warn("Cannot hold the delivery back: no {} header, so no manual acknowledgment to nack with",
                    KafkaHeaders.ACKNOWLEDGMENT);
            return false;
        }
        if (batch) {
            ack.nack(index, interval);
        } else {
            ack.nack(interval);
        }
        return true;
    }
}
