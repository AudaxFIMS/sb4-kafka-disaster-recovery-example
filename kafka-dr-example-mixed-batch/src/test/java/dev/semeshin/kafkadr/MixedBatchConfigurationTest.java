package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.Shape;
import dev.semeshin.kafkadr.handler.AuditRecordProcessor;
import dev.semeshin.kafkadr.handler.OrderBatchProcessor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the shipped {@code application.yml} really produces the arrangement this
 * example is meant to demonstrate. Without this the file could drift into a shape that
 * still starts but no longer illustrates anything — or into one that fails at startup,
 * which nobody notices until they run it.
 */
class MixedBatchConfigurationTest {

    private static KafkaClusterProperties properties;

    @BeforeAll
    static void loadShippedConfiguration() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        sources.forEach(environment.getPropertySources()::addFirst);

        properties = Binder.get(environment).bind("kafka-dr", KafkaClusterProperties.class).get();
    }

    @Test
    void oneConsumerBatchesAndTheOtherDoesNot() {
        BatchConfig orders = properties.getConsumers().get("order-events-consumer").getBatch();
        BatchConfig audit = properties.getConsumers().get("audit-events-consumer").getBatch();

        assertThat(orders.isEnabled()).isTrue();
        assertThat(orders.getMode()).isEqualTo(BatchConfig.Mode.SPLIT);
        assertThat(audit.isEnabled()).isFalse();
    }

    @Test
    void consumersDoNotShareATopicAndGroup() {
        // Container-level settings are resolved by (topic, group); sharing both would make
        // the two consumers indistinguishable and is rejected at startup.
        var orders = properties.getConsumers().get("order-events-consumer");
        var audit = properties.getConsumers().get("audit-events-consumer");

        assertThat(orders.getTopic()).isNotEqualTo(audit.getTopic());
        assertThat(orders.getGroup()).isNotEqualTo(audit.getGroup());
    }

    @Test
    void partialAcknowledgmentIsConfigured() {
        var orders = properties.getConsumers().get("order-events-consumer");

        // MANUAL would be rejected together with fail-batch: the successful prefix could
        // not be committed and every failure would reprocess the whole batch.
        assertThat(properties.resolveAckMode(orders)).isEqualTo(AckMode.MANUAL_IMMEDIATE);
        assertThat(orders.getBatch().getErrorPolicy()).isEqualTo(BatchConfig.ErrorPolicy.FAIL_BATCH);
    }

    @Test
    void handlersResolveToTheIntendedShapes() {
        // The registry constructor also runs the shape validation, so a mismatch between
        // a signature and its batch.mode fails here rather than at application startup.
        MessageHandlerRegistry registry = new MessageHandlerRegistry(
                List.of(new OrderBatchProcessor(), new AuditRecordProcessor()), properties);

        assertThat(registry.shapeOf("order-events-consumer")).isEqualTo(Shape.BATCH_OUTCOME);
        assertThat(registry.shapeOf("audit-events-consumer")).isEqualTo(Shape.SINGLE);
    }

    @Test
    void everyConsumerTopicHasAProducer() {
        List<String> consumed = properties.getConsumers().values().stream()
                .map(KafkaClusterProperties.ConsumerConfig::getTopic).toList();
        List<String> produced = properties.getProducers().values().stream()
                .map(KafkaClusterProperties.ProducerConfig::getTopic).toList();

        assertThat(produced).containsAll(consumed);
    }
}
