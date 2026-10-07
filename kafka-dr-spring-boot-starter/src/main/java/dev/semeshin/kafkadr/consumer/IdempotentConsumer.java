package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.function.Consumer;

/**
 * Wraps any message consumer with idempotency check.
 * Accepts Message<?> — the payload type is determined by the downstream handler.
 * The deduplication decision is fully delegated to the IdempotencyStore,
 * which receives the complete message (headers + payload).
 *
 * <p>Under manual acknowledgment the consumer also decides who commits and when the
 * seek-by-timestamp watermark may move — see {@link AckPolicy}. The watermark follows the
 * commit, never the handler: advancing it for a record whose offset was never committed
 * would make the seek after a failover skip records nothing redelivers.
 */
public class IdempotentConsumer implements Consumer<Message<?>> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final String consumerName;
    private final String clusterName;
    private final IdempotencyStore idempotencyStore;
    private final Consumer<Message<?>> delegate;
    private final LastProcessedTimestampTracker timestampTracker;
    private final AckObserver ackObserver;
    private final DependencyGate dependencyGate;

    /** Container-managed acknowledgment: the commit follows the listener returning. */
    public IdempotentConsumer(String consumerName,
                              String clusterName,
                              IdempotencyStore idempotencyStore,
                              Consumer<Message<?>> delegate,
                              LastProcessedTimestampTracker timestampTracker) {
        this(consumerName, clusterName, idempotencyStore, delegate, timestampTracker, AckPolicy.CONTAINER);
    }

    public IdempotentConsumer(String consumerName,
                              String clusterName,
                              IdempotencyStore idempotencyStore,
                              Consumer<Message<?>> delegate,
                              LastProcessedTimestampTracker timestampTracker,
                              AckPolicy ackPolicy) {
        this(consumerName, clusterName, idempotencyStore, delegate, timestampTracker, ackPolicy, DependencyGate.NONE);
    }

    /**
     * @param dependencyGate holds records back while a {@code depends-on} group is down;
     *                       requires a manual ack-mode, which startup validation enforces
     */
    public IdempotentConsumer(String consumerName,
                              String clusterName,
                              IdempotencyStore idempotencyStore,
                              Consumer<Message<?>> delegate,
                              LastProcessedTimestampTracker timestampTracker,
                              AckPolicy ackPolicy,
                              DependencyGate dependencyGate) {
        this.consumerName = consumerName;
        this.clusterName = clusterName;
        this.idempotencyStore = idempotencyStore;
        this.delegate = delegate;
        this.timestampTracker = timestampTracker;
        this.ackObserver = new AckObserver(ackPolicy, clusterName, consumerName,
                "Call Acknowledgment.acknowledge(), or set kafka-dr.consumers." + consumerName
                        + ".ack.owner=starter to let the starter do it (ack.async-acks=true if the handler "
                        + "acknowledges later, from another thread).");
        this.dependencyGate = dependencyGate == null ? DependencyGate.NONE : dependencyGate;
    }

    @Override
    public void accept(Message<?> msg) {
        // Checked before the idempotency mark: a record held back must come back unmarked.
        if (DependencyNacks.heldBack(msg, false, dependencyGate, clusterName, consumerName)) {
            return;
        }

        if (!idempotencyStore.tryProcess(clusterName, consumerName, msg)) {
            log.info("[{}][{}] Duplicate skipped: idempotency key={}", clusterName, consumerName, idempotencyStore.extractKey(msg));
            // The handler is not invoked, so under a manual ack-mode nobody else would ever
            // commit this offset — see AckObserver.acknowledgeUnhandled.
            ackObserver.acknowledgeUnhandled(msg);
            return;
        }

        log.info("[{}][{}] Processing: key={}", clusterName, consumerName, IdempotencyStore.kafkaKey(msg, null));

        // The handler only needs the wrapper when it is the one acknowledging. With
        // ack.owner=starter the message reaches it untouched.
        TrackingAcknowledgment ack = ackObserver.track(msg);
        Message<?> delivered = ackObserver.deliver(msg, ack);

        try {
            delegate.accept(delivered);
        } catch (RuntimeException e) {
            // The message was marked as processed before the handler ran. If the handler
            // failed, the mark has to go away or a redelivery would be dropped as a duplicate.
            // Nothing is acknowledged either, so the offset is not committed.
            idempotencyStore.rollback(clusterName, consumerName, List.of(msg));
            if (dependencyGate.isDependencyFailure(clusterName, e) && dependencyGate.mayHoldBack(clusterName, msg)) {
                // The handler could not reach a group it depends on. Throwing would spend the
                // retry budget in seconds and then skip the record; a nack brings it back.
                DependencyNacks.holdBack(msg, false, 0, dependencyGate, null, e, clusterName, consumerName);
                return;
            }
            throw e;
        }

        if (ack != null && ackObserver.policy().starterAcknowledges()) {
            ack.acknowledge();
        }
        if (ackObserver.commitObserved(ack)) {
            trackTimestamp(msg);
        }
    }

    private void trackTimestamp(Message<?> msg) {
        if (timestampTracker == null) return;
        if (!timestampTracker.advance(msg)) {
            log.debug("[{}][{}] Missing topic/partition/timestamp headers, watermark not advanced",
                    clusterName, consumerName);
        }
    }
}
