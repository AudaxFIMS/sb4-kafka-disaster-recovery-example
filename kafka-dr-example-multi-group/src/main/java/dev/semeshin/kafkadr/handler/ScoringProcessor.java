package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.model.OrderScore;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bridge {@code analytics -> core}: scores each bridged order and sends the score back to core.
 * The mirror image of {@link OrderBridgeProcessor}, with {@code depends-on: core} — so a core
 * outage pauses this direction while the other one waits on analytics.
 */
@Component
public class ScoringProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScoringProcessor.class);

    private final ResilientProducer producer;
    private final AtomicInteger scored = new AtomicInteger();

    public ScoringProcessor(ResilientProducer producer) {
        this.producer = producer;
    }

    public void scoreOrder(Message<OrderEvent> message) {
        String key = IdempotencyStore.kafkaKey(message, null);
        OrderScore score = OrderScore.of(message.getPayload());
        ResilientProducer.SendResult result = producer.to("order-scores").send(score, key).orThrow();
        log.info("Scored order {} = {}, sent analytics -> {} ({} total)",
                key, score.score(), result.cluster(), scored.incrementAndGet());
    }

    public int scoredCount() {
        return scored.get();
    }
}
