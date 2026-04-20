package com.example.kafkadr.idempotency;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ConfigurationCondition;

/**
 * Composite condition: kafka-dr.enabled=true AND kafka-dr.idempotency.type matches.
 * Used by IdempotencyStore implementations to ensure they're only created when DR is active.
 */
public class OnDrEnabledWithIdempotencyType extends AllNestedConditions {

    OnDrEnabledWithIdempotencyType() {
        super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
    static class DrEnabled {}

    public static class Redis extends AllNestedConditions {
        Redis() { super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN); }

        @ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
        static class DrEnabled {}

        @ConditionalOnProperty(name = "kafka-dr.idempotency.type", havingValue = "redis")
        static class TypeMatches {}
    }

    public static class InMemory extends AllNestedConditions {
        InMemory() { super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN); }

        @ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
        static class DrEnabled {}

        @ConditionalOnProperty(name = "kafka-dr.idempotency.type", havingValue = "in-memory", matchIfMissing = true)
        static class TypeMatches {}
    }
}
