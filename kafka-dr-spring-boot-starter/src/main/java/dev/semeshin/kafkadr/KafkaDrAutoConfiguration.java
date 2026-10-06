package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.AdminClientFactory;
import dev.semeshin.kafkadr.config.DefaultAdminClientFactory;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.consumer.TimestampSeekRebalanceListener;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import dev.semeshin.kafkadr.routing.FailoverStateStore;
import dev.semeshin.kafkadr.routing.InMemoryFailoverStateStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for Kafka DR framework.
 * Activated when kafka-dr.enabled=true.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@ComponentScan("dev.semeshin.kafkadr")
@EnableScheduling
public class KafkaDrAutoConfiguration {

    @Bean
    @ConditionalOnProperty(name = "kafka-dr.idempotency.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(IdempotencyStore.class)
    public InMemoryIdempotencyStore inMemoryIdempotencyStore(KafkaClusterProperties properties) {
        return new InMemoryIdempotencyStore(properties.getIdempotency().getKeyHeader());
    }

    @Bean
    @ConditionalOnMissingBean(FailoverStateStore.class)
    public InMemoryFailoverStateStore inMemoryFailoverStateStore() {
        return new InMemoryFailoverStateStore();
    }

    @Bean
    @ConditionalOnMissingBean(AdminClientFactory.class)
    public DefaultAdminClientFactory defaultAdminClientFactory() {
        return new DefaultAdminClientFactory();
    }

    /**
     * The single {@link ListenerContainerCustomizer} the Kafka binder accepts.
     *
     * <p>The binder injects exactly one — two beans of this type make the binder child
     * context fail to start — so everything the starter needs to do to a listener container
     * lives here rather than in one bean per concern.
     *
     * <p>Three things happen:
     * <ul>
     *   <li>with {@code failover.seek-by-timestamp}, the rebalance listener that seeks each
     *       assigned partition to its last committed timestamp;</li>
     *   <li>for batching consumers, {@code subBatchPerPartition}. A poll is grouped by
     *       partition, so without it a failure in the first partition truncates the commit
     *       prefix for every partition behind it — those records are redelivered and
     *       deduplicated for nothing. It lives on ContainerProperties and has no counterpart
     *       in KafkaConsumerProperties, so it cannot be set through YAML.</li>
     *   <li>the {@code kafka-dr.consumers.<name>.ack} settings — {@code async-acks},
     *       {@code sync-commits}, {@code count} and {@code time} — which are in the same
     *       position: real container settings the binder's properties cannot express.</li>
     * </ul>
     *
     * <p>The customizer only sees destination and group, hence the index — and hence the
     * startup check that (topic, group) is unique across consumers.
     */
    @Bean
    public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> kafkaDrContainerCustomizer(
            KafkaClusterProperties properties,
            LastProcessedTimestampTracker tracker) {

        Map<String, KafkaClusterProperties.ConsumerConfig> byDestinationAndGroup = new HashMap<>();
        for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers().values()) {
            byDestinationAndGroup.put(consumer.getTopic() + "|" + consumer.getGroup(), consumer);
        }
        boolean seekByTimestamp = properties.getFailover().isSeekByTimestamp();

        return (container, destination, group) -> {
            if (seekByTimestamp) {
                container.getContainerProperties().setConsumerRebalanceListener(
                        new TimestampSeekRebalanceListener(tracker));
            }
            KafkaClusterProperties.ConsumerConfig consumer =
                    byDestinationAndGroup.get(destination + "|" + group);
            if (consumer == null) {
                return;
            }
            if (consumer.getBatch().isEnabled()) {
                container.getContainerProperties().setSubBatchPerPartition(true);
            }
            applyAckSettings(container.getContainerProperties(), consumer.getAck());
        };
    }

    /**
     * Container-level acknowledgment settings. Each is left alone unless configured, so a
     * consumer that says nothing keeps the spring-kafka defaults. Values are validated at
     * startup by ConsumerConfigValidator; ackCount and ackTime would otherwise fail here
     * with spring-kafka's own assertion, far from the configuration that caused it.
     */
    private static void applyAckSettings(ContainerProperties container,
                                         KafkaClusterProperties.AckConfig ack) {
        if (ack == null) {
            return;
        }
        if (ack.getAsyncAcks() != null) {
            container.setAsyncAcks(ack.getAsyncAcks());
        }
        if (ack.getSyncCommits() != null) {
            container.setSyncCommits(ack.getSyncCommits());
        }
        if (ack.getCount() != null) {
            container.setAckCount(ack.getCount());
        }
        if (ack.getTime() != null) {
            container.setAckTime(ack.getTime());
        }
    }
}
