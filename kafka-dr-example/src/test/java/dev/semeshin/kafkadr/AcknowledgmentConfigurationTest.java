package dev.semeshin.kafkadr;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.consumer.AckPolicy;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry;
import dev.semeshin.kafkadr.consumer.MessageHandlerRegistry.Shape;
import dev.semeshin.kafkadr.handler.DemoAndOrderMessageProcessor;
import dev.semeshin.kafkadr.handler.LedgerMessageProcessor;
import dev.semeshin.kafkadr.handler.PaymentAndRawDataMessageProcessor;
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
 * Checks that the shipped {@code application.yml} really demonstrates starter-owned
 * acknowledgment. The two settings only mean something together: {@code ack-mode} alone
 * leaves the commit to a handler that has no acknowledgment code, and {@code ack.owner}
 * alone is a no-op because the container commits on its own.
 */
class AcknowledgmentConfigurationTest {

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
    void theLedgerConsumerCommitsManuallyAndLetsTheStarterDoIt() {
        AckPolicy policy = properties.resolveAckPolicy(properties.getConsumers().get("ledger-events-consumer"));

        assertThat(policy.ackMode()).isEqualTo(AckMode.MANUAL);
        assertThat(policy.owner()).isEqualTo(AckPolicy.Owner.STARTER);
        assertThat(policy.starterAcknowledges()).isTrue();
    }

    @Test
    void theOtherConsumersKeepTheContainerManagedDefault() {
        for (String name : List.of("demo-events-consumer", "order-events-consumer",
                "payment-events-consumer", "raw-telemetry-consumer")) {
            AckPolicy policy = properties.resolveAckPolicy(properties.getConsumers().get(name));

            assertThat(policy.ackMode()).as(name).isNull();
            assertThat(policy.isManual()).as(name).isFalse();
            assertThat(policy.commitFollowsListener()).as(name).isTrue();
        }
    }

    @Test
    void theLedgerHandlerIsAPlainRecordHandlerWithNoAcknowledgmentCode() {
        MessageHandlerRegistry registry = new MessageHandlerRegistry(
                List.of(new DemoAndOrderMessageProcessor(),
                        new PaymentAndRawDataMessageProcessor(),
                        new LedgerMessageProcessor()),
                properties);

        // Ownership is only a choice on the record path, and the handler stays a plain
        // Message<T> method — that is the entire point of the example.
        assertThat(registry.shapeOf("ledger-events-consumer")).isEqualTo(Shape.SINGLE);
        assertThat(properties.getConsumers().get("ledger-events-consumer").getBatch().isEnabled()).isFalse();
    }
}
