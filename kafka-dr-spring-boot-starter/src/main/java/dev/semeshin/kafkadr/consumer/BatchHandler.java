package dev.semeshin.kafkadr.consumer;

import org.springframework.messaging.Message;

import java.util.List;

/**
 * Drives one batch of deduplicated records through a user handler.
 *
 * <p>Implementations are built by {@link MessageHandlerRegistry}, which knows the
 * handler's shape and the configured error policy. Offset bookkeeping — watermarks,
 * idempotency rollback, partial commits — stays in {@link BatchIdempotentConsumer},
 * which is why the result reports positions rather than acting on them.
 */
@FunctionalInterface
public interface BatchHandler {

    /**
     * @param messages records to process, in poll order
     * @return what was and was not processed
     */
    Result process(List<Message<?>> messages);

    /**
     * @param firstUnprocessedIndex index into the list passed to {@link #process} from
     *                              which nothing may be committed; -1 when the whole list
     *                              can be committed
     * @param rollback              records whose idempotency marks must be released, so a
     *                              redelivery is not dropped as a duplicate. Authoritative
     *                              and complete: the caller never derives more from the
     *                              index, because outcomes need not be contiguous
     * @param failure               the exception that stopped processing, or null
     */
    record Result(int firstUnprocessedIndex, List<Message<?>> rollback, RuntimeException failure) {

        public static final Result COMPLETE = new Result(-1, List.of(), null);

        /** Processing stopped at {@code index}; everything from there on comes back. */
        public static Result stoppedAt(int index, List<Message<?>> messages, RuntimeException failure) {
            return new Result(index, List.copyOf(messages.subList(index, messages.size())), failure);
        }

        /** Records were passed over but the rest of the batch may still be committed. */
        public static Result skipping(List<Message<?>> skipped) {
            return skipped.isEmpty() ? COMPLETE : new Result(-1, List.copyOf(skipped), null);
        }

        public boolean stopped() {
            return firstUnprocessedIndex >= 0;
        }
    }
}
