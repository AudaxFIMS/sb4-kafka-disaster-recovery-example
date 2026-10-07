package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.AdminClientFactory;
import dev.semeshin.kafkadr.config.ClusterTopology;
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
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaListenerContainerCustomizer;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private static final Logger log = LoggerFactory.getLogger(KafkaDrAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = "kafka-dr.idempotency.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(IdempotencyStore.class)
    public InMemoryIdempotencyStore inMemoryIdempotencyStore(KafkaClusterProperties properties) {
        return new InMemoryIdempotencyStore(properties.getIdempotency().getKeyHeader(),
                properties.getIdempotency().getTtlSeconds());
    }

    @Bean
    @ConditionalOnMissingBean(FailoverStateStore.class)
    public InMemoryFailoverStateStore inMemoryFailoverStateStore() {
        return new InMemoryFailoverStateStore();
    }

    /**
     * The clusters resolved into groups, built once. The registrar validated the same
     * configuration before any bean existed; every component works on this one instance.
     */
    @Bean
    public ClusterTopology kafkaDrClusterTopology(KafkaClusterProperties properties) {
        return properties.topology();
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
     * <p>The container is matched to its consumer by binding name, which the binder passes to a
     * {@link KafkaListenerContainerCustomizer} and which is unique per consumer and cluster — the
     * same topic and consumer group may exist in two cluster groups, i.e. in two Kafkas, and
     * destination and group alone would not tell them apart. Destination and group remain the
     * fallback for callers that only supply those.
     */
    @Bean
    public KafkaListenerContainerCustomizer kafkaDrContainerCustomizer(
            KafkaClusterProperties properties,
            ClusterTopology topology,
            LastProcessedTimestampTracker tracker) {
        return new DrContainerCustomizer(properties, topology, tracker);
    }

    private static final class DrContainerCustomizer implements KafkaListenerContainerCustomizer {

        private final ClusterTopology topology;
        private final LastProcessedTimestampTracker tracker;
        /** For consumers outside any group — a configuration without clusters, in tests. */
        private final boolean globalSeekByTimestamp;
        private final Map<String, KafkaClusterProperties.ConsumerConfig> byBinding = new HashMap<>();
        private final Map<String, List<KafkaClusterProperties.ConsumerConfig>> byDestinationAndGroup = new HashMap<>();

        DrContainerCustomizer(KafkaClusterProperties properties, ClusterTopology topology,
                              LastProcessedTimestampTracker tracker) {
            this.topology = topology;
            this.tracker = tracker;
            this.globalSeekByTimestamp = properties.getFailover().isSeekByTimestamp();
            for (ClusterTopology.Group group : topology.groups()) {
                for (KafkaClusterProperties.ConsumerConfig consumer : group.consumers()) {
                    for (ClusterTopology.ClusterRef ref : group.clusters()) {
                        byBinding.put(ref.bindingName(consumer.getName()), consumer);
                    }
                }
            }
            for (KafkaClusterProperties.ConsumerConfig consumer : properties.getConsumers().values()) {
                byDestinationAndGroup
                        .computeIfAbsent(consumer.getTopic() + "|" + consumer.getGroup(), k -> new ArrayList<>())
                        .add(consumer);
            }
        }

        @Override
        public void configure(AbstractMessageListenerContainer<?, ?> container, String destination, String group,
                              ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
            String bindingName = extendedConsumerProperties == null ? null : extendedConsumerProperties.getBindingName();
            KafkaClusterProperties.ConsumerConfig consumer = bindingName == null ? null : byBinding.get(bindingName);
            apply(container, consumer != null ? consumer : byDestinationAndGroup(destination, group));
        }

        @Override
        public void configure(AbstractMessageListenerContainer<?, ?> container, String destination, String group) {
            apply(container, byDestinationAndGroup(destination, group));
        }

        private KafkaClusterProperties.ConsumerConfig byDestinationAndGroup(String destination, String group) {
            List<KafkaClusterProperties.ConsumerConfig> candidates = byDestinationAndGroup.get(destination + "|" + group);
            if (candidates == null) {
                return null;
            }
            if (candidates.size() > 1) {
                log.warn("Topic '{}' and consumer group '{}' belong to consumers {} in different cluster groups, and "
                                + "the container did not say which binding it serves. Container settings and "
                                + "seek-by-timestamp are not applied to it.", destination, group,
                        candidates.stream().map(KafkaClusterProperties.ConsumerConfig::getName).toList());
                return null;
            }
            return candidates.get(0);
        }

        private void apply(AbstractMessageListenerContainer<?, ?> container,
                           KafkaClusterProperties.ConsumerConfig consumer) {
            // A container that is not a DR consumer — or one that cannot be told apart — is left
            // alone entirely. Watermarks belong to consumers; nothing advances an unscoped one, so
            // a listener seeking with it could only move a partition back to a stale position.
            if (consumer == null) {
                return;
            }
            // seek-by-timestamp is a cluster-group setting: the group's failover is what moves
            // the consumer to a cluster whose offsets mean nothing to it.
            ClusterTopology.Group clusterGroup = topology.groupOf(consumer);
            boolean seekByTimestamp = clusterGroup == null
                    ? globalSeekByTimestamp
                    : clusterGroup.failover().isSeekByTimestamp();
            if (seekByTimestamp) {
                // Seek with the watermarks of this container's own consumer.
                container.getContainerProperties().setConsumerRebalanceListener(
                        new TimestampSeekRebalanceListener(tracker.forConsumer(consumer.getName())));
            }
            if (consumer.getBatch().isEnabled()) {
                container.getContainerProperties().setSubBatchPerPartition(true);
            }
            applyAckSettings(container.getContainerProperties(), consumer.getAck());
        }
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
