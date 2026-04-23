package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Auto-configuration for Kafka DR framework.
 * Activated when kafka-dr.enabled=true. Scans the framework's component packages
 * so the starter works regardless of the consuming application's base package.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@ComponentScan("dev.semeshin.kafkadr")
@EnableScheduling
public class KafkaDrAutoConfiguration {

    /**
     * Default in-memory idempotency store.
     * Automatically replaced when any custom IdempotencyStore bean is registered
     * in the consuming application (e.g. Redis, JDBC, etc.).
     */
    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    public InMemoryIdempotencyStore inMemoryIdempotencyStore() {
        return new InMemoryIdempotencyStore();
    }
}
