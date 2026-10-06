package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class EventMessageProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventMessageProcessor.class);

    public void processEvent(Message<String> message) {
        log.info("[{}] Processed event: key={}",
                message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                IdempotencyStore.kafkaKey(message, null));
    }
}
