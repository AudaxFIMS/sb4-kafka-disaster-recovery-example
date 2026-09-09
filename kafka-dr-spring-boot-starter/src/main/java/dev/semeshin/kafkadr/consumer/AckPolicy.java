package dev.semeshin.kafkadr.consumer;

import org.springframework.kafka.listener.ContainerProperties.AckMode;

/**
 * Who commits offsets on the record path, and whether the starter can observe the commit.
 *
 * <p>The batch paths have owned this decision since batching was added; on the record path it
 * used to be implicit, and the seek-by-timestamp watermark advanced whenever a handler
 * returned normally. That is wrong under manual acknowledgment: a watermark ahead of the
 * last committed offset makes the seek after a failover skip records that nothing will
 * redeliver. The policy is what lets {@link IdempotentConsumer} tie the watermark to the
 * commit instead of to the handler.
 *
 * @param ackMode   configured acknowledgment mode; null means the container default (BATCH)
 * @param owner     who calls {@link org.springframework.kafka.support.Acknowledgment#acknowledge()}
 * @param asyncAcks whether the container is configured for out-of-order acknowledgment, in
 *                  which case a handler that returns without acknowledging is legitimate
 *                  and must not be warned about
 */
public record AckPolicy(AckMode ackMode, Owner owner, boolean asyncAcks) {

    /** Who acknowledges on the record path. */
    public enum Owner {
        /** The handler reads the {@code kafka_acknowledgment} header and acknowledges itself. */
        HANDLER,
        /** The starter acknowledges after the handler returns, then advances the watermark. */
        STARTER
    }

    /** No ack-mode configured: the container commits as soon as the listener returns. */
    public static final AckPolicy CONTAINER = new AckPolicy(null, Owner.HANDLER, false);

    public AckPolicy {
        owner = owner == null ? Owner.HANDLER : owner;
    }

    /** True when the commit point is a call the application (or the starter) makes. */
    public boolean isManual() {
        return ackMode == AckMode.MANUAL || ackMode == AckMode.MANUAL_IMMEDIATE;
    }

    /** True when the starter acknowledges on the handler's behalf. */
    public boolean starterAcknowledges() {
        return isManual() && owner == Owner.STARTER;
    }

    /**
     * True for the modes whose commit coincides with the listener returning, which is the
     * only case in which the starter may advance the watermark without observing an ack.
     * TIME, COUNT and COUNT_TIME commit on the container's own schedule instead.
     */
    public boolean commitFollowsListener() {
        return ackMode == null || ackMode == AckMode.BATCH || ackMode == AckMode.RECORD;
    }
}
