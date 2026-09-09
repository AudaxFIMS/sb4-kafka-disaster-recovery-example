package dev.semeshin.kafkadr.consumer;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Batch consumer for {@code batch.mode: split} — the batch envelope is unpacked into
 * per-record messages, so deduplication, watermarks and existing {@code Message<T>}
 * handlers keep working exactly as they do without batching.
 *
 * <p>All per-batch state is local to {@link #accept}: with consumer
 * {@code concurrency > 1} the same bean is invoked from several container threads.
 */
public class BatchIdempotentConsumer implements Consumer<Message<?>> {

    private static final Logger log = LoggerFactory.getLogger(BatchIdempotentConsumer.class);

    private final String consumerName;
    private final String clusterName;
    private final IdempotencyStore idempotencyStore;
    private final BatchHandler handler;
    private final LastProcessedTimestampTracker timestampTracker;
    private final AckMode ackMode;
    private final boolean manualAck;
    private final boolean watermarkFollowsContainer;

    /**
     * @param ackMode configured acknowledgment mode; null means the container default (BATCH)
     */
    public BatchIdempotentConsumer(String consumerName,
                                   String clusterName,
                                   IdempotencyStore idempotencyStore,
                                   BatchHandler handler,
                                   LastProcessedTimestampTracker timestampTracker,
                                   AckMode ackMode) {
        this.consumerName = consumerName;
        this.clusterName = clusterName;
        this.idempotencyStore = idempotencyStore;
        this.handler = handler;
        this.timestampTracker = timestampTracker;
        this.ackMode = ackMode;
        this.manualAck = ackMode == AckMode.MANUAL || ackMode == AckMode.MANUAL_IMMEDIATE;
        // RECORD is silently treated as BATCH by the binder in batch mode; TIME, COUNT and
        // COUNT_TIME commit on their own schedule, which the starter cannot observe.
        this.watermarkFollowsContainer =
                ackMode == null || ackMode == AckMode.BATCH || ackMode == AckMode.RECORD;
    }

    @Override
    public void accept(Message<?> envelope) {
        boolean batch = BatchMessages.isBatch(envelope);
        // A late-initialized cluster whose binding was built in record mode would deliver
        // a single record here; handle it rather than failing with a ClassCastException.
        List<Message<?>> records = batch ? BatchMessages.split(envelope) : List.of(envelope);
        if (records.isEmpty()) {
            return;
        }

        List<Message<?>> toProcess = idempotencyStore.isEnabled()
                ? idempotencyStore.filterProcessable(clusterName, consumerName, records)
                : records;

        int duplicates = records.size() - toProcess.size();
        log.info("[{}][{}] Batch of {}: {} duplicates skipped, {} to process",
                clusterName, consumerName, records.size(), duplicates, toProcess.size());

        if (toProcess.isEmpty()) {
            // Nothing reaches the handler, so under a manual ack-mode nobody would commit
            // this batch: the offset would stall on an all-duplicate stretch, which is the
            // normal state right after a failover with replicated data. The watermark
            // follows that commit, exactly as it does for a processed batch.
            int ackedThrough = acknowledge(envelope, records.size(), records.size() - 1, true);
            if (ackedThrough >= 0) {
                advanceWatermarks(records, ackedThrough + 1);
            }
            return;
        }

        BatchHandler.Result result = handler.process(toProcess);

        // The rollback set is authoritative: verdicts need not be contiguous, so a record
        // marked done after a retried one keeps its mark and is deduplicated on redelivery.
        if (!result.rollback().isEmpty()) {
            idempotencyStore.rollback(clusterName, consumerName, result.rollback());
        }

        // Position in the batch the container holds, which is what offsets are counted in.
        int stoppedAt = result.stopped()
                ? indexByIdentity(records).getOrDefault(toProcess.get(result.firstUnprocessedIndex()), -1)
                : records.size();
        int lastCommittable = (stoppedAt < 0 ? 0 : stoppedAt) - 1;

        int ackedThrough = acknowledge(envelope, records.size(), lastCommittable, !result.stopped());

        // Watermark follows what was actually committed, never what was merely processed.
        // With manual acknowledgment a handler may hold the ack; moving the watermark
        // first would make seek-by-timestamp skip records whose offsets never landed.
        if (ackedThrough >= 0) {
            advanceWatermarks(records, ackedThrough + 1);
        }

        if (!result.stopped()) {
            return;
        }
        if (!batch || stoppedAt < 0) {
            throw result.failure();
        }
        throw new BatchListenerFailedException(
                "[%s][%s] redelivery required from record %d of %d"
                        .formatted(clusterName, consumerName, stoppedAt, records.size()),
                result.failure(), stoppedAt);
    }

    /**
     * Commits what may be committed and reports how far that reached.
     *
     * @param lastCommittable index of the last record that may be committed, or -1
     * @param complete        whether the whole batch was processed
     * @return index through which offsets are now committed, or -1 if nothing was
     */
    private int acknowledge(Message<?> envelope, int size, int lastCommittable, boolean complete) {
        if (!manualAck) {
            // TIME, COUNT and COUNT_TIME commit on their own schedule, with no point at
            // which the starter can observe it, so the watermark stays put and
            // seek-by-timestamp falls back to committed offsets after a failover.
            if (!watermarkFollowsContainer) {
                return -1;
            }
            if (complete) {
                return size - 1;
            }
            // Nothing is committed on failure. BatchListenerFailedException does not reach
            // DefaultErrorHandler intact: Spring Integration wraps it in a
            // MessageHandlingException before the container sees it, and the handler then
            // logs "Expected a BatchListenerFailedException; re-delivering full batch" and
            // replays everything. Verified against a live broker.
            //
            // So the watermark must not move either — advancing it for the successful
            // prefix would put it ahead of the last committed offset. Partial commits
            // require ack-mode=MANUAL_IMMEDIATE, where the starter commits explicitly.
            return -1;
        }

        Acknowledgment ack = envelope.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            log.warn("[{}][{}] ack-mode is {} but no {} header is present — offsets will not be committed",
                    clusterName, consumerName, ackMode, KafkaHeaders.ACKNOWLEDGMENT);
            return -1;
        }

        if (complete) {
            ack.acknowledge();
            return size - 1;
        }
        if (lastCommittable < 0) {
            return -1;
        }
        if (ackMode != AckMode.MANUAL_IMMEDIATE) {
            // Partial batch acknowledgment is only supported with MANUAL_IMMEDIATE, so
            // nothing can be committed here; the whole batch comes back.
            log.warn("[{}][{}] Records 0..{} succeeded but ack-mode={} cannot commit part of a batch; "
                            + "the whole batch will be redelivered. Use MANUAL_IMMEDIATE for partial commits.",
                    clusterName, consumerName, lastCommittable, ackMode);
            return -1;
        }
        ack.acknowledge(lastCommittable);
        return lastCommittable;
    }

    private void advanceWatermarks(List<Message<?>> records, int upToExclusive) {
        if (timestampTracker == null) {
            return;
        }
        for (int i = 0; i < upToExclusive; i++) {
            timestampTracker.advance(records.get(i));
        }
    }

    /**
     * Position of every record in the original batch. Identity-based on purpose:
     * {@link IdempotencyStore#filterProcessable} returns a subset of the very same
     * message instances, and equal-but-distinct records must not collapse.
     */
    private static Map<Message<?>, Integer> indexByIdentity(List<Message<?>> records) {
        Map<Message<?>, Integer> index = new IdentityHashMap<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            index.put(records.get(i), i);
        }
        return index;
    }
}
