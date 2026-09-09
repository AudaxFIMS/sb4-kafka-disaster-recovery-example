package dev.semeshin.kafkadr.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Routes per-consumer / per-producer properties into the two Spring Cloud Stream
 * namespaces they actually belong to:
 *
 * <ul>
 *   <li><b>core</b> — {@code spring.cloud.stream.bindings.{binding}.consumer|producer.*},
 *       the fields of {@link ConsumerProperties} / {@link ProducerProperties}
 *       (batch-mode, concurrency, max-attempts, back-off-*, ...)</li>
 *   <li><b>Kafka extension</b> — {@code spring.cloud.stream.kafka.bindings.{binding}.consumer|producer.*},
 *       the fields of {@link KafkaConsumerProperties} / {@link KafkaProducerProperties}
 *       (ack-mode, enable-dlq, configuration.*, sync, ...)</li>
 * </ul>
 *
 * The two field sets are disjoint, so the target namespace is derived from the
 * Spring Cloud Stream classes themselves rather than from a hand-maintained list —
 * it cannot drift when the dependency is upgraded.
 *
 * <p>A key that belongs to neither set is almost certainly a typo. It is routed to
 * the Kafka namespace (the historical behaviour) and reported with a warning, so it
 * no longer disappears silently.
 */
public final class BindingPropertyRouter {

    private static final Logger log = LoggerFactory.getLogger(BindingPropertyRouter.class);

    public enum Namespace { CORE, KAFKA }

    private static final Set<String> CORE_CONSUMER  = fieldNames(ConsumerProperties.class);
    private static final Set<String> CORE_PRODUCER  = fieldNames(ProducerProperties.class);
    private static final Set<String> KAFKA_CONSUMER = fieldNames(KafkaConsumerProperties.class);
    private static final Set<String> KAFKA_PRODUCER = fieldNames(KafkaProducerProperties.class);

    /**
     * Keys the starter owns: they are generated from kafka-dr configuration and
     * silently overriding them breaks binding lifecycle or payload handling.
     * Maps the offending key to the setting that should be used instead.
     */
    private static final Map<String, String> RESERVED_CONSUMER = Map.of(
            "autostartup", "the starter keeps consumer bindings stopped and starts them on cluster switch",
            "usenativedecoding", "derived from kafka-dr.consumers.<name>.content-type: native",
            "batchmode", "use kafka-dr.consumers.<name>.batch.enabled — the flag also decides which consumer function bean is registered",
            "destination", "derived from kafka-dr.consumers.<name>.topic",
            "group", "use kafka-dr.consumers.<name>.group",
            "binder", "derived from the kafka-dr.clusters entry name");

    private static final Map<String, String> RESERVED_PRODUCER = Map.of(
            "autostartup", "producer bindings are managed by the starter",
            "usenativeencoding", "derived from kafka-dr.producers.<name>.content-type: native",
            "destination", "derived from kafka-dr.producers.<name>.topic",
            "binder", "the target cluster is chosen per send by ResilientProducer");

    private BindingPropertyRouter() {
    }

    /**
     * @param key     flattened property key, e.g. {@code ack-mode} or {@code configuration.max.poll.records}
     * @param context consumer name, used only for log messages
     */
    public static Namespace forConsumerKey(String key, String context) {
        return route(key, context, "consumer", CORE_CONSUMER, KAFKA_CONSUMER);
    }

    public static Namespace forProducerKey(String key, String context) {
        return route(key, context, "producer", CORE_PRODUCER, KAFKA_PRODUCER);
    }

    /**
     * Fails fast when a starter-owned key is set by hand. Silently accepting these
     * produces failures far from their cause — a binding that never starts, or a
     * payload that arrives in an unexpected shape after a failover.
     */
    public static void checkNotReserved(String key, String context, boolean producer) {
        String head = normalize(head(key));
        String reason = (producer ? RESERVED_PRODUCER : RESERVED_CONSUMER).get(head);
        if (reason != null) {
            throw new IllegalStateException(
                    "kafka-dr.%ss.%s.properties.%s is managed by the starter and must not be set directly — %s"
                            .formatted(producer ? "producer" : "consumer", context, key, reason));
        }
    }

    private static Namespace route(String key, String context, String kind,
                                   Set<String> core, Set<String> kafka) {
        String head = normalize(head(key));
        if (core.contains(head)) {
            return Namespace.CORE;
        }
        if (!kafka.contains(head)) {
            log.warn("[{}] Unknown {} property '{}' — routed to the Kafka binder namespace, "
                            + "where it will be ignored. Check the spelling.",
                    context, kind, key);
        }
        return Namespace.KAFKA;
    }

    /** First dot-separated segment: {@code configuration.max.poll.records} -> {@code configuration}. */
    private static String head(String key) {
        int dot = key.indexOf('.');
        return dot < 0 ? key : key.substring(0, dot);
    }

    /** {@code backOffInitialInterval} / {@code back-off-initial-interval} -> {@code backoffinitialinterval}. */
    static String normalize(String key) {
        return key.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
    }

    private static Set<String> fieldNames(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
                .filter(f -> !f.isSynthetic() && !Modifier.isStatic(f.getModifiers()))
                .map(Field::getName)
                .map(BindingPropertyRouter::normalize)
                .collect(Collectors.toUnmodifiableSet());
    }
}
