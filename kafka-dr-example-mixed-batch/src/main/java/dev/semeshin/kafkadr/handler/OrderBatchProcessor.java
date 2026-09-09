package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.BatchOutcome;
import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Batch consumer for {@code order-events}.
 *
 * <p>Returning {@link BatchOutcome} is what buys per-record granularity: offsets commit as
 * a per-partition watermark, so the starter can only acknowledge a contiguous prefix, but
 * the verdicts tell it which records to release from the idempotency store. Records marked
 * {@code done} after a retried one are redelivered and then deduplicated — the store is
 * what remembers they are already finished.
 */
@Component
public class OrderBatchProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderBatchProcessor.class);

    private final AtomicInteger batches = new AtomicInteger();
    private final AtomicInteger records = new AtomicInteger();
    /** Order ids already handed back once, so a demo retry resolves instead of looping. */
    private final Set<String> retriedOnce = ConcurrentHashMap.newKeySet();

    /**
     * Demonstration rules, chosen to exercise all three verdicts:
     * <ul>
     *   <li>{@code amount < 0} — unprocessable, {@code discard}: retrying cannot help,
     *       so the record keeps its mark and is never handed back;</li>
     *   <li>customer {@code "flaky"} — {@code retry} on first delivery: the mark is
     *       released and Kafka redelivers the record, which then succeeds. Retrying it
     *       forever would only show a loop; the point is the round trip;</li>
     *   <li>everything else — {@code done}.</li>
     * </ul>
     */
    public BatchOutcome processOrders(List<Message<OrderEvent>> messages) {
        int batch = batches.incrementAndGet();
        log.info("Batch #{}: {} orders", batch, messages.size());

        BatchOutcome outcome = BatchOutcome.of(messages);
        for (int i = 0; i < messages.size(); i++) {
            OrderEvent order = messages.get(i).getPayload();
            Object partition = messages.get(i).getHeaders().get(KafkaHeaders.RECEIVED_PARTITION);

            if (order.amount() < 0) {
                log.warn("  [{}] {} discarded: negative amount {}", partition, order.orderId(), order.amount());
                outcome.discard(i, new IllegalArgumentException("negative amount"));
                continue;
            }
            if ("flaky".equals(order.customer()) && retriedOnce.add(order.orderId())) {
                log.warn("  [{}] {} handed back for redelivery", partition, order.orderId());
                outcome.retry(i, new IllegalStateException("downstream unavailable"));
                continue;
            }

            log.info("  [{}] {} processed, amount={} (total {})",
                    partition, order.orderId(), order.amount(), records.incrementAndGet());
            outcome.done(i);
        }
        return outcome;
    }

    public int batchCount() {
        return batches.get();
    }

    public int recordCount() {
        return records.get();
    }
}
