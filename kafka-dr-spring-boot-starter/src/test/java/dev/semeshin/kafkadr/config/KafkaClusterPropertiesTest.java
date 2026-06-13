package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.ClusterConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClusterPropertiesTest {

    @Test
    void idempotencyIsEnabledByDefault() {
        KafkaClusterProperties props = new KafkaClusterProperties();

        assertThat(props.isIdempotencyEnabled()).isTrue();
    }

    @Test
    void idempotencyCanBeDisabledViaFlag() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.getIdempotency().setEnabled(false);

        assertThat(props.isIdempotencyEnabled()).isFalse();
    }

    @Test
    void effectiveEnvironmentMergesDefaultsBelowClusterOverrides() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setDefaultEnvironment(nested(
                "spring.cloud.stream.kafka.binder", nested(
                        "configuration", Map.of(
                                "schema.registry.url", "http://default:8081",
                                "request.timeout.ms", "5000"
                        )
                )
        ));

        ClusterConfig primary = new ClusterConfig();
        primary.setBootstrapServers("kafka-primary:9092");
        primary.setEnvironment(nested(
                "spring.cloud.stream.kafka.binder", nested(
                        "configuration", Map.of(
                                "schema.registry.url", "http://primary-sr:8081"
                        )
                )
        ));
        props.setClusters(Map.of("primary", primary));

        Map<String, String> env = props.getEffectiveEnvironment("primary");

        assertThat(env)
                .containsEntry("spring.cloud.stream.kafka.binder.configuration.schema.registry.url",
                        "http://primary-sr:8081")
                .containsEntry("spring.cloud.stream.kafka.binder.configuration.request.timeout.ms",
                        "5000");
    }

    @Test
    void effectiveEnvironmentForUnknownClusterReturnsOnlyDefaults() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setDefaultEnvironment(nested("a", Map.of("b", "c")));

        Map<String, String> env = props.getEffectiveEnvironment("nonexistent");

        assertThat(env).containsExactly(Map.entry("a.b", "c"));
    }

    @Test
    void effectiveConsumerPropertiesMergeDefaultsAndOverrides() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setDefaultConsumerProperties(nested(
                "configuration", Map.of("max.poll.records", "100")
        ));

        ConsumerConfig consumer = new ConsumerConfig();
        consumer.setProperties(nested(
                "configuration", Map.of(
                        "max.poll.records", "500",
                        "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer"
                )
        ));

        Map<String, String> merged = props.getEffectiveConsumerProperties(consumer);

        assertThat(merged)
                .containsEntry("configuration.max.poll.records", "500")
                .containsEntry("configuration.value.deserializer",
                        "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    }

    @Test
    void effectiveProducerPropertiesMergeDefaultsAndOverrides() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setDefaultProducerProperties(nested(
                "sync", true,
                "configuration", Map.of("acks", "all")
        ));

        ProducerConfig producer = new ProducerConfig();
        producer.setProperties(nested(
                "configuration", Map.of("acks", "1")
        ));

        Map<String, String> merged = props.getEffectiveProducerProperties(producer);

        assertThat(merged)
                .containsEntry("sync", "true")
                .containsEntry("configuration.acks", "1");
    }

    @Test
    void functionNameCombinesCamelTopicAndCapitalizedCluster() {
        assertThat(KafkaClusterProperties.functionName("order-events", "primary"))
                .isEqualTo("orderEventsPrimary");
        assertThat(KafkaClusterProperties.functionName("payment-events", "us-east"))
                .isEqualTo("paymentEventsUs-east");
    }

    @Test
    void bindingNameAppendsInZero() {
        assertThat(KafkaClusterProperties.bindingName("demo-events", "secondary"))
                .isEqualTo("demoEventsSecondary-in-0");
    }

    @Test
    void producerBindingNameStripsDotsToCamelCase() {
        assertThat(KafkaClusterProperties.producerBindingName("ax123.test.event"))
                .isEqualTo("ax123TestEvent");
        assertThat(KafkaClusterProperties.producerBindingName("simple-topic"))
                .isEqualTo("simpleTopic");
    }

    @Test
    void nullValuesInEnvironmentAreSkipped() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("present", "value");
        env.put("absent", null);
        props.setDefaultEnvironment(env);

        Map<String, String> flat = props.getEffectiveEnvironment("any");

        assertThat(flat).containsEntry("present", "value").doesNotContainKey("absent");
    }

    @Test
    void clusterConfigDefaultsArePopulated() {
        ClusterConfig cfg = new ClusterConfig();
        assertThat(cfg.getPriority()).isEqualTo(100);
        assertThat(cfg.getEnvironment()).isEmpty();
        assertThat(cfg.getBootstrapServers()).isNull();
    }

    @Test
    void allTopLevelGettersAndSettersRoundTrip() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setAutoCreateTopics(true);
        props.setDefaultConsumerProperties(Map.of("k", "v"));

        KafkaClusterProperties.FailoverConfig failover = new KafkaClusterProperties.FailoverConfig();
        failover.setSeekByTimestamp(true);
        failover.setFailbackAfter("23:00:00");
        props.setFailover(failover);

        KafkaClusterProperties.IdempotencyConfig idem = new KafkaClusterProperties.IdempotencyConfig();
        idem.setTtlSeconds(99);
        idem.setKeyPrefix("p");
        idem.setKeyHeader("h");
        props.setIdempotency(idem);

        KafkaClusterProperties.HealthCheckConfig hc = new KafkaClusterProperties.HealthCheckConfig();
        hc.setIntervalMs(1000);
        hc.setTimeoutMs(500);
        hc.setFailureThreshold(7);
        hc.setRecoveryThreshold(2);
        hc.setDeepProbe(true);
        hc.setDeepProbeMinNodes(2);
        hc.setDeepProbeMinIsr(3);
        props.setHealthCheck(hc);

        props.setDefaultProducerProperties(Map.of("p", "q"));

        assertThat(props.isAutoCreateTopics()).isTrue();
        assertThat(props.getDefaultConsumerProperties()).containsEntry("k", "v");
        assertThat(props.getDefaultProducerProperties()).containsEntry("p", "q");
        assertThat(props.getDefaultEnvironment()).isEmpty();

        assertThat(props.getFailover().isSeekByTimestamp()).isTrue();
        assertThat(props.getFailover().getFailbackAfter()).isEqualTo("23:00:00");

        assertThat(props.getIdempotency().getTtlSeconds()).isEqualTo(99);
        assertThat(props.getIdempotency().getKeyPrefix()).isEqualTo("p");
        assertThat(props.getIdempotency().getKeyHeader()).isEqualTo("h");

        assertThat(props.getHealthCheck().getIntervalMs()).isEqualTo(1000);
        assertThat(props.getHealthCheck().getTimeoutMs()).isEqualTo(500);
        assertThat(props.getHealthCheck().getFailureThreshold()).isEqualTo(7);
        assertThat(props.getHealthCheck().getRecoveryThreshold()).isEqualTo(2);
        assertThat(props.getHealthCheck().isDeepProbe()).isTrue();
        assertThat(props.getHealthCheck().getDeepProbeMinNodes()).isEqualTo(2);
        assertThat(props.getHealthCheck().getDeepProbeMinIsr()).isEqualTo(3);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nested(Object... kv) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put((String) kv[i], kv[i + 1]);
        }
        return map;
    }
}
