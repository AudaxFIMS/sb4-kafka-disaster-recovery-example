package dev.semeshin.kafkadr.idempotency;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyStoreTest {

    private static Message<?> messageWithKey(String key) {
        return MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.RECEIVED_KEY, key)
                .build();
    }

    @Test
    void disabledStoreReportsItselfAsDisabled() {
        assertThat(IdempotencyStore.DISABLED.isEnabled()).isFalse();
    }

    @Test
    void disabledStoreNeverDetectsDuplicates() {
        Message<?> msg = messageWithKey("msg-1");

        assertThat(IdempotencyStore.DISABLED.tryProcess("primary", "orders-consumer", msg)).isTrue();
        assertThat(IdempotencyStore.DISABLED.tryProcess("primary", "orders-consumer", msg)).isTrue();
    }

    @Test
    void disabledStoreExtractsNoKey() {
        assertThat(IdempotencyStore.DISABLED.extractKey(messageWithKey("msg-1"))).isNull();
    }

    @Test
    void storeIsEnabledByDefault() {
        IdempotencyStore custom = (clusterName, consumerName, message) -> true;

        assertThat(custom.isEnabled()).isTrue();
    }

    @Test
    void defaultExtractKeyUsesKafkaRecordKey() {
        IdempotencyStore custom = (clusterName, consumerName, message) -> true;

        assertThat(custom.extractKey(messageWithKey("k-1"))).isEqualTo("k-1");
    }

    @Test
    void blankCustomKeyHeaderFallsBackToKafkaKey() {
        assertThat(IdempotencyStore.kafkaKey(messageWithKey("k-1"), "  ")).isEqualTo("k-1");
    }
}
