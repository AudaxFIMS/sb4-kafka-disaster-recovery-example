package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Record consumer for {@code audit-events}, in the same application as the batch one.
 *
 * <p>Nothing here is aware that another consumer batches: the signature, deduplication and
 * acknowledgment are exactly what they were before batching existed. That is the point of
 * the example — {@code batch-mode} is a binding property, and each consumer gets its own
 * binding, listener container and function bean.
 */
@Component
public class AuditRecordProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(AuditRecordProcessor.class);

    private final AtomicInteger processed = new AtomicInteger();

    public void processAudit(Message<String> message) {
        Object key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
        Object partition = message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION);

        log.info("Audit [{}] key={}: {} (total {})",
                partition, key, message.getPayload(), processed.incrementAndGet());
    }

    public int processedCount() {
        return processed.get();
    }
}
