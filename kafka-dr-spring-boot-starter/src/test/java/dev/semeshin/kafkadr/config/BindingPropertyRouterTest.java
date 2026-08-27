package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.BindingPropertyRouter.Namespace;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

class BindingPropertyRouterTest {

    @Test
    void coreConsumerKeysGoToTheCoreNamespace() {
        assertThat(BindingPropertyRouter.forConsumerKey("concurrency", "orders")).isEqualTo(Namespace.CORE);
        assertThat(BindingPropertyRouter.forConsumerKey("max-attempts", "orders")).isEqualTo(Namespace.CORE);
        assertThat(BindingPropertyRouter.forConsumerKey("back-off-initial-interval", "orders")).isEqualTo(Namespace.CORE);
        assertThat(BindingPropertyRouter.forConsumerKey("header-mode", "orders")).isEqualTo(Namespace.CORE);
    }

    @Test
    void kafkaConsumerKeysGoToTheKafkaNamespace() {
        assertThat(BindingPropertyRouter.forConsumerKey("ack-mode", "orders")).isEqualTo(Namespace.KAFKA);
        assertThat(BindingPropertyRouter.forConsumerKey("enable-dlq", "orders")).isEqualTo(Namespace.KAFKA);
        assertThat(BindingPropertyRouter.forConsumerKey("dlq-name", "orders")).isEqualTo(Namespace.KAFKA);
        assertThat(BindingPropertyRouter.forConsumerKey("start-offset", "orders")).isEqualTo(Namespace.KAFKA);
    }

    @Test
    void routingUsesOnlyTheFirstSegment() {
        assertThat(BindingPropertyRouter.forConsumerKey("configuration.max.poll.records", "orders"))
                .isEqualTo(Namespace.KAFKA);
        assertThat(BindingPropertyRouter.forConsumerKey("retryable-exceptions.java.lang.IllegalStateException", "orders"))
                .isEqualTo(Namespace.CORE);
    }

    @Test
    void camelCaseAndKebabCaseResolveIdentically() {
        assertThat(BindingPropertyRouter.forConsumerKey("backOffInitialInterval", "orders"))
                .isEqualTo(BindingPropertyRouter.forConsumerKey("back-off-initial-interval", "orders"));
        assertThat(BindingPropertyRouter.forProducerKey("partitionKeyExpression", "orders"))
                .isEqualTo(BindingPropertyRouter.forProducerKey("partition-key-expression", "orders"));
    }

    @Test
    void producerKeysSplitAcrossNamespaces() {
        assertThat(BindingPropertyRouter.forProducerKey("partition-count", "orders")).isEqualTo(Namespace.CORE);
        assertThat(BindingPropertyRouter.forProducerKey("required-groups", "orders")).isEqualTo(Namespace.CORE);
        // sync drives failover detection; misrouting it to core would silently make sends async
        assertThat(BindingPropertyRouter.forProducerKey("sync", "orders")).isEqualTo(Namespace.KAFKA);
        assertThat(BindingPropertyRouter.forProducerKey("compression-type", "orders")).isEqualTo(Namespace.KAFKA);
    }

    @Test
    void unknownKeysFallBackToTheKafkaNamespace() {
        assertThat(BindingPropertyRouter.forConsumerKey("no-such-property", "orders")).isEqualTo(Namespace.KAFKA);
    }

    @Test
    void starterOwnedConsumerKeysAreRejected() {
        assertThatThrownBy(() -> BindingPropertyRouter.checkNotReserved("batch-mode", "orders", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.consumers.orders.properties.batch-mode")
                .hasMessageContaining("batch.enabled");

        assertThatThrownBy(() -> BindingPropertyRouter.checkNotReserved("auto-startup", "orders", false))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> BindingPropertyRouter.checkNotReserved("group", "orders", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.consumers.<name>.group");
    }

    @Test
    void starterOwnedProducerKeysAreRejected() {
        assertThatThrownBy(() -> BindingPropertyRouter.checkNotReserved("use-native-encoding", "orders", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kafka-dr.producers.orders.properties.use-native-encoding");
    }

    @Test
    void ordinaryKeysAreNotReserved() {
        assertThatCode(() -> BindingPropertyRouter.checkNotReserved("ack-mode", "orders", false))
                .doesNotThrowAnyException();
        assertThatCode(() -> BindingPropertyRouter.checkNotReserved("configuration.max.poll.records", "orders", false))
                .doesNotThrowAnyException();
        assertThatCode(() -> BindingPropertyRouter.checkNotReserved("sync", "orders", true))
                .doesNotThrowAnyException();
    }

    @Test
    void namespacesAreDisjoint() {
        // The whole routing scheme rests on this: if Spring Cloud Stream ever adds the
        // same field to both classes, a key would become ambiguous and the router would
        // silently prefer core. Fail here instead, on upgrade.
        assertThat(intersection(ConsumerProperties.class, KafkaConsumerProperties.class))
                .as("consumer core/extension field overlap").isEmpty();
        assertThat(intersection(ProducerProperties.class, KafkaProducerProperties.class))
                .as("producer core/extension field overlap").isEmpty();
    }

    private static Set<String> intersection(Class<?> a, Class<?> b) {
        Set<String> other = names(b);
        return names(a).stream().filter(other::contains).collect(Collectors.toSet());
    }

    private static Set<String> names(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
                .filter(f -> !f.isSynthetic() && !Modifier.isStatic(f.getModifiers()))
                .map(Field::getName)
                .map(n -> n.replaceAll("[^A-Za-z0-9]", "").toLowerCase())
                .collect(Collectors.toSet());
    }
}
