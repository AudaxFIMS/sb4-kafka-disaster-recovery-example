package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.Shape;
import dev.semeshin.kafkadr.flow.BillingGateway;
import dev.semeshin.kafkadr.handler.BillingBatchProcessor;
import dev.semeshin.kafkadr.handler.InvoiceProcessor;
import dev.semeshin.kafkadr.handler.OrderFlowProcessor;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Pins the shipped {@code application.yml} to the arrangement this example demonstrates, so it
 * cannot drift into a shape that still boots but no longer illustrates anything.
 */
class IntegrationFlowConfigurationTest {

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
    void oneRecordConsumerFeedsAFlowAndOneBatchConsumerFeedsAGateway() {
        assertThat(properties.getConsumers().get("flow-orders-consumer").getBatch().isEnabled()).isFalse();

        BatchConfig billing = properties.getConsumers().get("flow-billing-consumer").getBatch();
        assertThat(billing.isEnabled()).isTrue();
        assertThat(billing.getMode()).isEqualTo(BatchConfig.Mode.SPLIT);
        assertThat(billing.getErrorPolicy()).isEqualTo(BatchConfig.ErrorPolicy.FAIL_BATCH);
    }

    @Test
    void batchPathCanAcknowledgeAPrefix() {
        var billing = properties.getConsumers().get("flow-billing-consumer");
        assertThat(properties.resolveAckMode(billing)).isEqualTo(AckMode.MANUAL_IMMEDIATE);
    }

    @Test
    void handlersResolveToTheIntendedShapes() {
        MessageHandlerRegistry registry = new MessageHandlerRegistry(
                List.of(new OrderFlowProcessor(new DirectChannel()),
                        new BillingBatchProcessor(mock(BillingGateway.class), mock(ResilientProducer.class)),
                        new InvoiceProcessor()),
                properties);

        assertThat(registry.shapeOf("flow-orders-consumer")).isEqualTo(Shape.SINGLE);
        assertThat(registry.shapeOf("flow-billing-consumer")).isEqualTo(Shape.BATCH_OUTCOME);
        assertThat(registry.shapeOf("flow-invoices-consumer")).isEqualTo(Shape.SINGLE);
    }

    @Test
    void everyTopicTheFlowsPublishIntoHasAProducer() {
        List<String> produced = properties.getProducers().values().stream()
                .map(KafkaClusterProperties.ProducerConfig::getTopic).toList();

        // orderFlow and BillingBatchProcessor both publish to flow-invoices; without the
        // producer entry the send would fail with "No producer configured for topic".
        assertThat(produced).contains("flow-invoices", "flow-orders", "flow-billing");
    }

    @Test
    void recordPathBoundsItsRetryChainExplicitly() {
        // A handler failure propagates now, so leaving max-attempts at the default 3 would
        // silently multiply every poison record by three before the container even sees it.
        assertThat(properties.getEffectiveConsumerProperties(
                properties.getConsumers().get("flow-orders-consumer")))
                .containsEntry("max-attempts", "1");
    }
}
