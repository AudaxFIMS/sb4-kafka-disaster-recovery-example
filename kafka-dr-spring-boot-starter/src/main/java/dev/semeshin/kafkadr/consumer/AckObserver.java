package dev.semeshin.kafkadr.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Makes the commit observable for the consumers whose handler owns it, and answers the one
 * question the timestamp watermark depends on: was this delivery actually committed.
 *
 * <p>Shared by the record path and the standard batch path — the two places where the
 * starter hands an {@link Acknowledgment} to application code and never hears back. The
 * split batch consumer does not use it: there the starter computes the commit point itself
 * and already knows the answer.
 *
 * <p>One instance per consumer bean, so the warn-once flags are scoped to a consumer and
 * cluster. Everything that varies per delivery is returned to the caller rather than kept
 * here: with {@code concurrency > 1} the same bean runs on several container threads.
 */
final class AckObserver {

    private static final Logger log = LoggerFactory.getLogger(AckObserver.class);

    private final AckPolicy policy;
    private final String clusterName;
    private final String consumerName;
    private final String unacknowledgedHint;
    private final AtomicBoolean missingHeaderWarned = new AtomicBoolean();
    private final AtomicBoolean unacknowledgedWarned = new AtomicBoolean();

    /**
     * @param unacknowledgedHint what to suggest when a handler returns without acknowledging;
     *                           the remedy differs per path, the diagnosis does not
     */
    AckObserver(AckPolicy policy, String clusterName, String consumerName, String unacknowledgedHint) {
        this.policy = policy == null ? AckPolicy.CONTAINER : policy;
        this.clusterName = clusterName;
        this.consumerName = consumerName;
        this.unacknowledgedHint = unacknowledgedHint;
    }

    AckPolicy policy() {
        return policy;
    }

    /**
     * Wraps the container's {@link Acknowledgment} so the commit becomes observable, or
     * returns null when there is nothing to observe: a container-managed ack-mode, or a
     * missing header despite a manual one.
     */
    TrackingAcknowledgment track(Message<?> message) {
        if (!policy.isManual()) {
            return null;
        }
        Acknowledgment ack = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            warnOnce(missingHeaderWarned,
                    "ack-mode is {} but no {} header is present — offsets will not be committed",
                    policy.ackMode(), KafkaHeaders.ACKNOWLEDGMENT);
            return null;
        }
        return new TrackingAcknowledgment(ack);
    }

    /**
     * The message to hand to the handler. The wrapper only matters to whoever acknowledges,
     * so with {@code ack.owner=starter} the message is passed through untouched.
     */
    Message<?> deliver(Message<?> message, TrackingAcknowledgment ack) {
        if (ack == null || policy.starterAcknowledges()) {
            return message;
        }
        return MessageBuilder.fromMessage(message).setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack).build();
    }

    /**
     * Commits a record the handler will never see — one the idempotency store filtered out
     * as already processed.
     *
     * <p>Nobody else can commit it: the handler is not invoked, so under a manual ack-mode
     * the offset would stay uncommitted no matter who owns the acknowledgment. On a
     * partition whose tail is all duplicates — the normal state right after a failover with
     * replicated data — that leaves the group's committed offset behind those records
     * indefinitely, and once their idempotency marks expire the redelivery is processed for
     * real.
     *
     * <p>The watermark is deliberately not the caller's to move here: the record was
     * processed by an earlier delivery, which already put its timestamp there.
     *
     * @return whether an acknowledgment was actually made
     */
    boolean acknowledgeUnhandled(Message<?> message) {
        TrackingAcknowledgment ack = track(message);
        if (ack == null) {
            return false;
        }
        ack.acknowledge();
        return true;
    }

    /**
     * Whether the offsets behind this delivery can be considered committed, which is the
     * only condition under which the watermark may move.
     */
    boolean commitObserved(TrackingAcknowledgment ack) {
        if (!policy.isManual()) {
            // TIME, COUNT and COUNT_TIME commit on their own schedule, with no point at
            // which the starter can observe it, so the watermark stays put and
            // seek-by-timestamp falls back to committed offsets after a failover.
            return policy.commitFollowsListener();
        }
        if (ack == null) {
            return false;
        }
        if (ack.isAcknowledged()) {
            return true;
        }
        // A nack is a deliberate redelivery request, and with async acks the handler is
        // allowed to acknowledge after returning. Neither is a forgotten acknowledgment.
        if (!ack.isNacked() && !policy.asyncAcks()) {
            warnOnce(unacknowledgedWarned,
                    "Handler returned without acknowledging under ack-mode={}. Offsets are not committed and "
                            + "the timestamp watermark stays put. {}",
                    policy.ackMode(), unacknowledgedHint);
        }
        return false;
    }

    private void warnOnce(AtomicBoolean flag, String message, Object... args) {
        if (flag.compareAndSet(false, true)) {
            Object[] prefixed = new Object[args.length + 2];
            prefixed[0] = clusterName;
            prefixed[1] = consumerName;
            System.arraycopy(args, 0, prefixed, 2, args.length);
            log.warn("[{}][{}] " + message, prefixed);
        }
    }
}
