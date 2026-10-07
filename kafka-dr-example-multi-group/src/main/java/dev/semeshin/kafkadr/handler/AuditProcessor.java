package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Topic {@code audit} in both groups, read under the same consumer group name in each. They are
 * two topics in two Kafkas: the starter tells the two containers apart by binding name, and the
 * counters show each record landing only in the group it was sent to.
 */
@Component
public class AuditProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(AuditProcessor.class);

    private final AtomicInteger core = new AtomicInteger();
    private final AtomicInteger analytics = new AtomicInteger();

    public void auditCore(Message<String> message) {
        log.info("Audit in core, key={}: {} ({} total)",
                IdempotencyStore.kafkaKey(message, null), message.getPayload(), core.incrementAndGet());
    }

    public void auditAnalytics(Message<String> message) {
        log.info("Audit in analytics, key={}: {} ({} total)",
                IdempotencyStore.kafkaKey(message, null), message.getPayload(), analytics.incrementAndGet());
    }

    public int coreCount() {
        return core.get();
    }

    public int analyticsCount() {
        return analytics.get();
    }
}
