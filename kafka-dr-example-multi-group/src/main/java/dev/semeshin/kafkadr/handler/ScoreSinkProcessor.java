package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.model.OrderScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * End of the round trip: scores arriving back in {@code core}. Counting distinct orders makes
 * "nothing lost" checkable from {@code GET /api/status}: it should reach the number of orders
 * sent. A double cannot show here by construction — compare the delivery counters of the bridge
 * and the scorer for that.
 */
@Component
public class ScoreSinkProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScoreSinkProcessor.class);

    private final Set<String> completedOrders = ConcurrentHashMap.newKeySet();

    public void recordScore(Message<OrderScore> message) {
        OrderScore score = message.getPayload();
        completedOrders.add(score.orderId());
        log.info("Round trip complete for order {} (key={}): score {} ({} orders)",
                score.orderId(), IdempotencyStore.kafkaKey(message, null), score.score(), completedOrders.size());
    }

    public int completedCount() {
        return completedOrders.size();
    }
}
