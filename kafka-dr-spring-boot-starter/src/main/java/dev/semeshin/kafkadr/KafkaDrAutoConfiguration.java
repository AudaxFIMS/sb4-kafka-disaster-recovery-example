package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.DynamicBindingRegistrar;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.consumer.TimestampSeekRebalanceListener;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Auto-configuration for Kafka DR framework.
 * Activated when kafka-dr.enabled=true.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@ComponentScan("dev.semeshin.kafkadr")
@EnableScheduling
public class KafkaDrAutoConfiguration {

    /**
     * Must be static — ensures BeanDefinitionRegistryPostProcessor runs before
     * Spring Cloud Function processes function definitions. This is critical when
     * the consuming app has a different base package than the starter.
     */
    @Bean
    static DynamicBindingRegistrar dynamicBindingRegistrar() {
        return new DynamicBindingRegistrar();
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    public InMemoryIdempotencyStore inMemoryIdempotencyStore() {
        return new InMemoryIdempotencyStore();
    }

    @Bean
    @ConditionalOnProperty(name = "kafka-dr.failover.seek-by-timestamp", havingValue = "true")
    public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> timestampSeekCustomizer(
            LastProcessedTimestampTracker tracker) {
        return (container, dest, group) ->
                container.getContainerProperties().setConsumerRebalanceListener(
                        new TimestampSeekRebalanceListener(tracker));
    }
}
