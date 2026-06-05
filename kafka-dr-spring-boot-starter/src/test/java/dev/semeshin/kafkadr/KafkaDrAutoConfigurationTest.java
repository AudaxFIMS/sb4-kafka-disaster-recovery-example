package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.DefaultAdminClientFactory;
import dev.semeshin.kafkadr.consumer.LastProcessedTimestampTracker;
import dev.semeshin.kafkadr.idempotency.IdempotencyStore;
import dev.semeshin.kafkadr.idempotency.InMemoryIdempotencyStore;
import dev.semeshin.kafkadr.routing.FailoverStateStore;
import dev.semeshin.kafkadr.routing.InMemoryFailoverStateStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaDrAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaDrAutoConfiguration.class));

    @Test
    void autoConfigurationDoesNotLoadWhenEnabledPropertyAbsent() {
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(IdempotencyStore.class));
    }

    @Test
    void autoConfigurationDoesNotLoadWhenEnabledIsFalse() {
        runner.withPropertyValues("kafka-dr.enabled=false")
                .run(ctx -> assertThat(ctx).doesNotHaveBean(IdempotencyStore.class));
    }

    @Test
    void inMemoryIdempotencyStoreFactoryBeanCreatesValidInstance() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        InMemoryIdempotencyStore store = cfg.inMemoryIdempotencyStore();

        assertThat(store).isNotNull();
        assertThat(store.tryProcess("c", "id-1")).isTrue();
        assertThat(store.tryProcess("c", "id-1")).isFalse();
    }

    @Test
    void inMemoryFailoverStateStoreFactoryBeanCreatesValidInstance() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        InMemoryFailoverStateStore store = cfg.inMemoryFailoverStateStore();

        assertThat(store).isNotNull();
        store.save(new FailoverStateStore.FailoverState("primary", Instant.parse("2025-01-15T14:00:00Z")));
        Optional<FailoverStateStore.FailoverState> loaded = store.load();
        assertThat(loaded).isPresent();
        assertThat(loaded.get().activeCluster()).isEqualTo("primary");
    }

    @Test
    void defaultAdminClientFactoryBeanIsRegistered() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        DefaultAdminClientFactory factory = cfg.defaultAdminClientFactory();

        assertThat(factory).isNotNull();
    }

    @Test
    void timestampSeekCustomizerInstallsRebalanceListenerOnContainer() {
        KafkaDrAutoConfiguration cfg = new KafkaDrAutoConfiguration();
        LastProcessedTimestampTracker tracker = new LastProcessedTimestampTracker(null);

        ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer =
                cfg.timestampSeekCustomizer(tracker);

        AbstractMessageListenerContainer<?, ?> container = mock(AbstractMessageListenerContainer.class);
        ContainerProperties props = new ContainerProperties("topic");
        when(container.getContainerProperties()).thenReturn(props);

        customizer.configure(container, "topic", "group");

        assertThat(props.getConsumerRebalanceListener())
                .isInstanceOf(ConsumerAwareRebalanceListener.class);

        @SuppressWarnings("unchecked")
        Consumer<Object, Object> consumer = mock(Consumer.class);
        ((ConsumerAwareRebalanceListener) props.getConsumerRebalanceListener())
                .onPartitionsAssigned(consumer, List.<TopicPartition>of());
    }
}
