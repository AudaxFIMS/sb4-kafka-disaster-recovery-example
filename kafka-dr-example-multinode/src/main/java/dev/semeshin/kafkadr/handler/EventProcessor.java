package dev.semeshin.kafkadr.handler;

import dev.semeshin.kafkadr.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class EventProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

    public void processEvent(Message<String> message) {
        log.info("[{}] key={}, payload={}",
                message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                message.getHeaders().get(KafkaHeaders.RECEIVED_KEY),
                message.getPayload());
    }
}
