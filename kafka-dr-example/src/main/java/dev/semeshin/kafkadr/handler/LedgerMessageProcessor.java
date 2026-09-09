package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Example of a consumer that commits manually, with {@code ack.owner: starter}.
 *
 * <p>The topic runs with {@code ack-mode: MANUAL}, so the container does not commit when the
 * listener returns — something has to acknowledge. Here that something is the starter: it
 * acknowledges after this method returns normally, and only then advances the
 * seek-by-timestamp watermark, so the watermark can never point past a committed offset.
 * A handler that throws is not acknowledged at all, its idempotency mark is rolled back and
 * Kafka redelivers the record.
 *
 * <p>The point of the example is what is <em>not</em> here. With the default
 * {@code ack.owner: handler} the same method would have to read the header and commit
 * itself, and forgetting to do so would silently stop the consumer group from committing:
 *
 * <pre>{@code
 * public void processLedgerEntry(Message<String> message) {
 *     log.info(...);
 *     message.getHeaders()
 *             .get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class)
 *             .acknowledge();
 * }
 * }</pre>
 *
 * <p>Try it with:
 * {@code curl -X POST "http://localhost:8080/api/messages/ledger-events?message=entry-1"}
 */
@Component
public class LedgerMessageProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(LedgerMessageProcessor.class);

    public void processLedgerEntry(Message<String> message) {
        // The Kafka key arrives as byte[]; the starter's helper is the same UTF-8 conversion
        // its own logs use, so a record can be followed across both.
        log.info("[ledger-events] key={}, entry={}",
                IdempotencyStore.kafkaKey(message, null), message.getPayload());
    }
}
