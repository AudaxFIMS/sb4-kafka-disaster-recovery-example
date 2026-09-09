package dev.semeshin.kafkadr.consumer;

import org.springframework.messaging.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-record outcomes reported back by a batch handler.
 *
 * <p>Kafka commits a per-partition watermark, not a set of records, so a handler cannot
 * acknowledge an arbitrary subset. What it can do is say what happened to each record and
 * let the starter translate that into the two mechanisms that do exist: a contiguous
 * offset commit, and the idempotency store as the sparse "already done" set.
 *
 * <pre>{@code
 * public BatchOutcome processOrders(List<Message<OrderEvent>> messages) {
 *     BatchOutcome outcome = BatchOutcome.of(messages);
 *     for (int i = 0; i < messages.size(); i++) {
 *         try {
 *             handle(messages.get(i));
 *             outcome.done(i);
 *         } catch (PoisonPayloadException e) {
 *             outcome.discard(i, e);   // closed for good, do not redeliver
 *         } catch (TransientException e) {
 *             outcome.retry(i, e);     // hand back to Kafka
 *         }
 *     }
 *     return outcome;
 * }
 * }</pre>
 *
 * <p>The three outcomes are not interchangeable. {@code done} and {@code discard} both
 * keep the record's idempotency mark — one because it succeeded, the other because
 * repeating it would fail again — while {@code retry} releases the mark so the
 * redelivery is not dropped as a duplicate.
 *
 * <p>A record left unmarked counts as {@code retry}. Assuming success would silently drop
 * whatever the handler forgot; assuming failure costs a redelivery the idempotency store
 * absorbs.
 */
public final class BatchOutcome {

    /** What the handler decided about one record. */
    public enum Verdict {
        /** Processed successfully. */
        DONE,
        /** Cannot be processed and must not be retried — route it to a DLQ if you keep one. */
        DISCARD,
        /** Not processed; hand it back to Kafka. This is also the default for unmarked records. */
        RETRY
    }

    private final List<Message<?>> messages;
    private final Verdict[] verdicts;
    private final Throwable[] causes;

    private BatchOutcome(List<Message<?>> messages) {
        this.messages = List.copyOf(messages);
        this.verdicts = new Verdict[messages.size()];
        this.causes = new Throwable[messages.size()];
    }

    /** Starts an outcome for the batch the handler was given. */
    public static BatchOutcome of(List<? extends Message<?>> messages) {
        return new BatchOutcome(List.copyOf(messages));
    }

    public BatchOutcome done(int index) {
        return mark(index, Verdict.DONE, null);
    }

    public BatchOutcome discard(int index, Throwable cause) {
        return mark(index, Verdict.DISCARD, cause);
    }

    public BatchOutcome retry(int index, Throwable cause) {
        return mark(index, Verdict.RETRY, cause);
    }

    private BatchOutcome mark(int index, Verdict verdict, Throwable cause) {
        if (index < 0 || index >= verdicts.length) {
            throw new IndexOutOfBoundsException(
                    "Record index %d is outside the batch of %d".formatted(index, verdicts.length));
        }
        verdicts[index] = verdict;
        causes[index] = cause;
        return this;
    }

    public int size() {
        return verdicts.length;
    }

    /** Verdict for a record; RETRY when the handler never marked it. */
    public Verdict verdictAt(int index) {
        Verdict verdict = verdicts[index];
        return verdict == null ? Verdict.RETRY : verdict;
    }

    public Throwable causeAt(int index) {
        return causes[index];
    }

    /** Number of records the handler never marked, reported so the mistake is visible. */
    public int unmarkedCount() {
        int count = 0;
        for (Verdict verdict : verdicts) {
            if (verdict == null) {
                count++;
            }
        }
        return count;
    }

    /**
     * Position of the first record that has to come back. Everything before it can be
     * committed; nothing from it on can be, because offsets move as a watermark.
     *
     * @return the index, or -1 when no record needs redelivery
     */
    public int firstRetryIndex() {
        for (int i = 0; i < verdicts.length; i++) {
            if (verdictAt(i) == Verdict.RETRY) {
                return i;
            }
        }
        return -1;
    }

    /** Records whose idempotency marks must be released before redelivery. */
    public List<Message<?>> retried() {
        List<Message<?>> retried = new ArrayList<>();
        for (int i = 0; i < verdicts.length; i++) {
            if (verdictAt(i) == Verdict.RETRY) {
                retried.add(messages.get(i));
            }
        }
        return retried;
    }

    /** First cause recorded for a retried record, used to explain the redelivery. */
    public Throwable firstRetryCause() {
        int index = firstRetryIndex();
        return index < 0 ? null : causes[index];
    }
}
