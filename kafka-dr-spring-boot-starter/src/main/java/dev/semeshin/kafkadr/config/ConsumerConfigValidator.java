package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.AckConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.AckPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.util.HashMap;
import java.util.Map;

/**
 * Checks consumer configuration at startup.
 *
 * <p>Everything caught here would otherwise surface as data loss during a failover — the
 * moment when the cause is hardest to find. A startup failure with an explanation costs a
 * deploy; a watermark that silently outran the committed offset costs the records between
 * them. Combinations that are merely suboptimal are logged instead of rejected.
 *
 * <p>Rules that depend on the handler's signature live in {@code MessageHandlerRegistry},
 * which is the only place the method is known.
 */
final class ConsumerConfigValidator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfigValidator.class);

    /** Default max.poll.interval.ms; a large batch has to fit inside it. */
    private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000;

    private ConsumerConfigValidator() {
    }

    static void validate(KafkaClusterProperties props) {
        validate(props, ClusterTopology.from(props));
    }

    static void validate(KafkaClusterProperties props, ClusterTopology topology) {
        Map<String, String> seenTopicGroups = new HashMap<>();

        for (ConsumerConfig consumer : props.getConsumers().values()) {
            String name = consumer.getName();
            checkUniqueTopicAndGroup(consumer, topology, seenTopicGroups);
            checkIdempotencyOverride(props, consumer);
            // Independent of batching: a DLQ that cannot be written to loses records
            // whether the consumer batches or not.
            checkDlqSerializer(props, consumer);

            AckMode ackMode = props.resolveAckMode(consumer);
            // Acknowledgment is not a batch-only concern: the record path can be manual too,
            // and until it was validated a forgotten acknowledge() showed up as a consumer
            // group that quietly stopped committing.
            checkAckOwnership(consumer, ackMode);
            checkContainerAckSettings(consumer, ackMode, failoverOf(props, topology, consumer));
            checkDependsOn(consumer, topology, ackMode);

            BatchConfig batch = consumer.getBatch();
            if (!batch.isEnabled()) {
                continue;
            }

            checkStandardMode(props, consumer, batch);
            checkAckMode(consumer, batch, ackMode);
            checkPartialCommitReachable(consumer, batch, ackMode);
            checkPollInterval(props, consumer, batch);
            checkDeserializer(props, consumer);

            log.info("[{}] Batch enabled: mode={}, error-policy={}, ack-mode={}, max-records={}",
                    name, batch.getMode(), batch.getErrorPolicy(),
                    ackMode == null ? "BATCH (container default)" : ackMode, batch.getMaxRecords());
        }
    }

    /**
     * Who commits under a manual ack-mode, and whether that is even expressible.
     *
     * <p>On the batch paths ownership is fixed: {@code split} is acknowledged by the starter,
     * {@code standard} by the handler that receives the envelope. Only the record path has a
     * choice, so {@code ack.owner} is rejected anywhere else rather than silently ignored.
     */
    private static void checkAckOwnership(ConsumerConfig consumer, AckMode ackMode) {
        String name = consumer.getName();
        boolean batch = consumer.getBatch().isEnabled();
        boolean manual = ackMode == AckMode.MANUAL || ackMode == AckMode.MANUAL_IMMEDIATE;
        AckPolicy.Owner owner = consumer.getAck() == null || consumer.getAck().getOwner() == null
                ? AckPolicy.Owner.HANDLER
                : consumer.getAck().getOwner();

        if (owner == AckPolicy.Owner.STARTER) {
            if (batch) {
                throw new IllegalStateException(
                        ("Consumer '%s' sets ack.owner=starter with batching enabled. Ownership is not a choice "
                                + "there: in batch.mode=split the starter already computes the commit point, and "
                                + "in batch.mode=standard the handler owns the envelope and its acknowledgment. "
                                + "Remove kafka-dr.consumers.%s.ack.owner.").formatted(name, name));
            }
            if (!manual) {
                log.warn("[{}] ack.owner=starter has no effect with ack-mode={}: the container commits once the "
                                + "listener returns. Set properties.ack-mode=MANUAL to move the commit into the "
                                + "starter.", name, ackMode == null ? "BATCH (container default)" : ackMode);
            } else {
                log.info("[{}] ack.owner=starter: the starter acknowledges after the handler returns, then "
                        + "advances the timestamp watermark", name);
            }
        } else if (manual && !batch) {
            log.warn("[{}] ack-mode={} in record mode: the handler owns the commit and must call "
                            + "Acknowledgment.acknowledge() on the kafka_acknowledgment header, or offsets are "
                            + "never committed. Set kafka-dr.consumers.{}.ack.owner=starter to have the starter "
                            + "acknowledge instead.", name, ackMode, name);
        }

        if (!batch && (ackMode == AckMode.TIME || ackMode == AckMode.COUNT || ackMode == AckMode.COUNT_TIME)) {
            log.warn("[{}] ack-mode={} commits on its own schedule, which the starter cannot observe. The "
                            + "timestamp watermark is left alone, so seek-by-timestamp falls back to committed "
                            + "offsets after a failover.", name, ackMode);
        }
    }

    /**
     * The {@code ContainerProperties} settings the customizer applies. They are checked here
     * because the container is built inside a binder child context, sometimes only at
     * failover: spring-kafka's own assertion would fire there, hours after the deploy.
     */
    private static void checkContainerAckSettings(ConsumerConfig consumer, AckMode ackMode,
                                                  KafkaClusterProperties.FailoverConfig failover) {
        AckConfig ack = consumer.getAck();
        if (ack == null) {
            return;
        }
        String name = consumer.getName();

        if (ack.getCount() != null && ack.getCount() <= 0) {
            throw new IllegalStateException(
                    "Consumer '%s' has ack.count=%d; spring-kafka requires ackCount > 0"
                            .formatted(name, ack.getCount()));
        }
        if (ack.getTime() != null && ack.getTime() <= 0) {
            throw new IllegalStateException(
                    "Consumer '%s' has ack.time=%d; spring-kafka requires ackTime > 0"
                            .formatted(name, ack.getTime()));
        }
        if (ack.getCount() != null && ackMode != AckMode.COUNT && ackMode != AckMode.COUNT_TIME) {
            log.warn("[{}] ack.count applies only to ack-mode COUNT and COUNT_TIME, current ack-mode is {} — "
                    + "the setting is ignored", name, ackMode == null ? "BATCH (container default)" : ackMode);
        }
        if (ack.getTime() != null && ackMode != AckMode.TIME && ackMode != AckMode.COUNT_TIME) {
            log.warn("[{}] ack.time applies only to ack-mode TIME and COUNT_TIME, current ack-mode is {} — "
                    + "the setting is ignored", name, ackMode == null ? "BATCH (container default)" : ackMode);
        }
        if (Boolean.TRUE.equals(ack.getAsyncAcks())) {
            if (ackMode != AckMode.MANUAL && ackMode != AckMode.MANUAL_IMMEDIATE) {
                log.warn("[{}] ack.async-acks applies only to ack-mode MANUAL and MANUAL_IMMEDIATE, current "
                                + "ack-mode is {} — the setting is ignored",
                        name, ackMode == null ? "BATCH (container default)" : ackMode);
            } else if (ack.getOwner() == AckPolicy.Owner.STARTER) {
                log.warn("[{}] ack.async-acks with ack.owner=starter: the starter acknowledges on the consumer "
                        + "thread before the listener returns, so acknowledgments are never out of order", name);
            } else if (failover.isSeekByTimestamp()) {
                // Every other setting that silences the watermark says so at startup; this
                // one used to be the exception, and it silently removes a configured feature.
                log.warn("[{}] ack.async-acks with failover.seek-by-timestamp: the acknowledgment arrives after "
                                + "the handler returns, so the starter never observes the commit and the timestamp "
                                + "watermark is not advanced. Seek-by-timestamp falls back to committed offsets "
                                + "after a failover.", name);
            }
        }
    }

    /**
     * {@code depends-on} only keeps its promise — nothing skipped while a dependency is down —
     * through negative acknowledgment, and a nack exists only under a manual ack-mode without
     * out-of-order commits. Accepting it anywhere else would leave a configuration that reads as
     * protected and loses records exactly when the dependency fails.
     */
    private static void checkDependsOn(ConsumerConfig consumer, ClusterTopology topology, AckMode ackMode) {
        if (consumer.getDependsOn() == null || consumer.getDependsOn().isEmpty()) {
            return;
        }
        String name = consumer.getName();
        ClusterTopology.Group own = topology.groupOf(consumer);
        for (String raw : consumer.getDependsOn()) {
            String group = raw == null ? "" : raw.trim();
            if (group.isEmpty()) {
                throw new IllegalStateException("Consumer '%s' has an empty entry in depends-on".formatted(name));
            }
            if (topology.isEmpty()) {
                continue;
            }
            if (topology.groups().stream().noneMatch(g -> g.name().equals(group))) {
                throw new IllegalStateException("Consumer '%s' depends on cluster group '%s', which is not configured. "
                        .formatted(name, group) + "Known groups: " + topology.groups().stream()
                        .map(ClusterTopology.Group::name).toList());
            }
            if (own != null && own.name().equals(group)) {
                throw new IllegalStateException(
                        ("Consumer '%s' depends on its own cluster group '%s'. Its own group's failover already "
                                + "covers that; depends-on is for the groups its handler writes to.")
                                .formatted(name, group));
            }
        }
        if (consumer.getDependsOnMaxHoldMs() <= 0) {
            throw new IllegalStateException("Consumer '%s' has depends-on-max-hold-ms=%d; it must be positive"
                    .formatted(name, consumer.getDependsOnMaxHoldMs()));
        }
        if (consumer.getDependsOnNackIntervalMs() <= 0) {
            throw new IllegalStateException("Consumer '%s' has depends-on-nack-interval-ms=%d; it must be positive"
                    .formatted(name, consumer.getDependsOnNackIntervalMs()));
        }

        BatchConfig batch = consumer.getBatch();
        boolean split = batch.isEnabled() && batch.getMode() == BatchConfig.Mode.SPLIT;
        boolean manual = ackMode == AckMode.MANUAL || ackMode == AckMode.MANUAL_IMMEDIATE;
        if (split && ackMode != AckMode.MANUAL_IMMEDIATE) {
            throw new IllegalStateException(
                    ("Consumer '%s' uses depends-on with batch.mode=split, which needs ack-mode=MANUAL_IMMEDIATE: a "
                            + "batch stopped by an unavailable dependency is held back from the stopping record, and "
                            + "only MANUAL_IMMEDIATE commits the prefix before it. Current ack-mode: %s.")
                            .formatted(name, ackMode == null ? "BATCH (container default)" : ackMode));
        }
        if (split && batch.getErrorPolicy() == BatchConfig.ErrorPolicy.SKIP_FAILED) {
            throw new IllegalStateException(
                    ("Consumer '%s' combines depends-on with batch.error-policy=skip-failed. A record that fails "
                            + "because the dependency went down mid-batch would be skipped and committed with the "
                            + "rest of the batch instead of held back — the loss depends-on exists to prevent. Use "
                            + "error-policy=fail-batch.").formatted(name));
        }
        if (!manual) {
            throw new IllegalStateException(
                    ("Consumer '%s' uses depends-on, which needs ack-mode=MANUAL or MANUAL_IMMEDIATE. Records are "
                            + "held back with a negative acknowledgment, which does not count as a delivery attempt; "
                            + "without one, a dependency outage spends the retry budget in seconds and the records "
                            + "are skipped. Current ack-mode: %s. Set kafka-dr.consumers.%s.properties.ack-mode=MANUAL "
                            + "(and ack.owner=starter to keep acknowledgment out of the handler).")
                            .formatted(name, ackMode == null ? "BATCH (container default)" : ackMode, name));
        }
        if (consumer.getAck() != null && Boolean.TRUE.equals(consumer.getAck().getAsyncAcks())) {
            throw new IllegalStateException(
                    ("Consumer '%s' combines depends-on with ack.async-acks=true. spring-kafka does not support "
                            + "negative acknowledgment with out-of-order commits, so records could not be held back.")
                            .formatted(name));
        }
        if (batch.isEnabled() && batch.getMode() == BatchConfig.Mode.STANDARD) {
            log.warn("[{}] depends-on with batch.mode=standard: only the pause and the gate apply. A send that "
                    + "fails inside the handler is the handler's to hold back — nack the envelope's "
                    + "Acknowledgment; a thrown exception is retried and the batch is then skipped", name);
        }
        log.info("[{}] depends-on {}: paused while any of them is unavailable, held-back records redelivered "
                        + "after {} ms, held at most {} ms while the dependency is available",
                name, consumer.getDependsOn(), consumer.getDependsOnNackIntervalMs(), consumer.getDependsOnMaxHoldMs());
    }

    /** The consumer's group settings, or the global ones while no cluster is configured. */
    private static KafkaClusterProperties.FailoverConfig failoverOf(KafkaClusterProperties props,
                                                                   ClusterTopology topology,
                                                                   ConsumerConfig consumer) {
        ClusterTopology.Group group = topology.groupOf(consumer);
        return group == null ? props.getFailover() : group.failover();
    }

    /**
     * Two consumers on the same topic and group are indistinguishable to a
     * {@code ListenerContainerCustomizer}, which only sees destination and group — so
     * per-consumer container settings would be applied to the wrong one. Across cluster
     * groups the pair may repeat: those are different Kafkas, and the customizer tells their
     * containers apart by binding name, which is unique per consumer and cluster — which is also
     * why late-initialized bindings must carry theirs.
     */
    private static void checkUniqueTopicAndGroup(ConsumerConfig consumer, ClusterTopology topology,
                                                 Map<String, String> seen) {
        ClusterTopology.Group clusterGroup = topology.groupOf(consumer);
        String key = (clusterGroup == null ? "" : clusterGroup.name()) + "|" + consumer.getTopic()
                + "|" + consumer.getGroup();
        String previous = seen.put(key, consumer.getName());
        if (previous != null) {
            throw new IllegalStateException(
                    ("Consumers '%s' and '%s' share topic '%s' and group '%s'. Container-level settings are "
                            + "resolved by (topic, group), so the two cannot be told apart. Give them "
                            + "different groups.")
                            .formatted(previous, consumer.getName(), consumer.getTopic(), consumer.getGroup()));
        }
    }

    private static void checkIdempotencyOverride(KafkaClusterProperties props, ConsumerConfig consumer) {
        if (Boolean.TRUE.equals(consumer.getIdempotencyEnabled()) && !props.isIdempotencyEnabled()) {
            log.warn("[{}] idempotency-enabled=true is ignored: kafka-dr.idempotency.enabled is false, "
                    + "so no IdempotencyStore bean exists", consumer.getName());
        }
    }

    private static void checkStandardMode(KafkaClusterProperties props, ConsumerConfig consumer,
                                          BatchConfig batch) {
        if (batch.getMode() != BatchConfig.Mode.STANDARD) {
            return;
        }
        String name = consumer.getName();
        if (props.isIdempotencyEnabled(consumer)) {
            throw new IllegalStateException(
                    ("Consumer '%s' uses batch.mode=standard, which hands the raw batch envelope to the "
                            + "handler and cannot deduplicate per record. Set "
                            + "kafka-dr.consumers.%s.idempotency-enabled=false to acknowledge that, or use "
                            + "batch.mode=split.").formatted(name, name));
        }
        AckMode ackMode = props.resolveAckMode(consumer);
        if (ackMode == AckMode.MANUAL || ackMode == AckMode.MANUAL_IMMEDIATE) {
            log.warn("[{}] batch.mode=standard with ack-mode={}: the handler owns acknowledgment and must "
                            + "call Acknowledgment.acknowledge() itself, or offsets will never be committed",
                    name, ackMode);
        }
    }

    private static void checkAckMode(ConsumerConfig consumer, BatchConfig batch, AckMode ackMode) {
        String name = consumer.getName();
        if (ackMode == null) {
            return;
        }
        switch (ackMode) {
            case RECORD -> log.warn("[{}] ack-mode=RECORD is not applied in batch mode — the binder skips it "
                    + "and the container keeps its BATCH default, which behaves the same way", name);
            case TIME, COUNT, COUNT_TIME -> log.warn(
                    "[{}] ack-mode={} commits on its own schedule, which the starter cannot observe. The "
                            + "timestamp watermark is left alone, so seek-by-timestamp falls back to "
                            + "committed offsets after a failover.", name, ackMode);
            case MANUAL -> {
                if (batch.getErrorPolicy() == BatchConfig.ErrorPolicy.FAIL_BATCH) {
                    throw new IllegalStateException(
                            ("Consumer '%s' combines ack-mode=MANUAL with error-policy=fail-batch. Partial "
                                    + "batch acknowledgment requires MANUAL_IMMEDIATE, so the successful "
                                    + "prefix could not be committed and the whole batch would be "
                                    + "reprocessed on every failure. Use MANUAL_IMMEDIATE, or "
                                    + "error-policy=skip-failed.").formatted(name));
                }
            }
            default -> { }
        }
    }

    /**
     * Only MANUAL_IMMEDIATE gives a partial commit. With the container-managed modes the
     * BatchListenerFailedException the starter throws never reaches DefaultErrorHandler
     * intact — Spring Integration wraps it first — so the handler logs "Expected a
     * BatchListenerFailedException; re-delivering full batch" and the whole batch is
     * replayed. Correct, but every failure reprocesses the successful prefix, which the
     * idempotency store then has to filter out again.
     */
    private static void checkPartialCommitReachable(ConsumerConfig consumer, BatchConfig batch,
                                                    AckMode ackMode) {
        if (batch.getErrorPolicy() == BatchConfig.ErrorPolicy.FAIL_BATCH
                && ackMode != AckMode.MANUAL_IMMEDIATE) {
            log.warn("[{}] error-policy=fail-batch with ack-mode={}: the whole batch is redelivered on "
                            + "failure, not just the records from the failing one. Use "
                            + "ack-mode=MANUAL_IMMEDIATE for partial commits.",
                    consumer.getName(), ackMode == null ? "BATCH (container default)" : ackMode);
        }
    }

    private static void checkPollInterval(KafkaClusterProperties props, ConsumerConfig consumer,
                                          BatchConfig batch) {
        Integer maxRecords = batch.getMaxRecords();
        if (maxRecords == null || maxRecords <= 100) {
            return;
        }
        String configured = props.getEffectiveConsumerProperties(consumer)
                .get("configuration.max.poll.interval.ms");
        if (configured == null) {
            log.warn("[{}] batch.max-records={} with the default max.poll.interval.ms ({} ms): "
                            + "max-records times the per-record processing time must fit inside it, or the "
                            + "consumer is evicted from the group mid-batch and the whole batch is redelivered.",
                    consumer.getName(), maxRecords, DEFAULT_MAX_POLL_INTERVAL_MS);
        }
    }

    /**
     * A DLQ that cannot be written to is worse than no DLQ: the record it was meant to
     * preserve is dropped instead, and only after the retries are exhausted — long after
     * the configuration that caused it.
     *
     * <p>With native decoding the binder refuses to publish unless the DLQ producer carries
     * its own serializers, because the payload is no longer {@code byte[]}. Verified against
     * the binder: the check runs only when {@code useNativeDecoding} is set, and it fails
     * when the DLQ producer configuration is empty or missing the serializer.
     */
    private static void checkDlqSerializer(KafkaClusterProperties props, ConsumerConfig consumer) {
        Map<String, String> effective = props.getEffectiveConsumerProperties(consumer);
        boolean dlqEnabled = "true".equalsIgnoreCase(findByNormalizedHead(effective, "enabledlq"));
        if (!dlqEnabled || !"native".equalsIgnoreCase(consumer.getContentType())) {
            return;
        }
        boolean hasSerializer = effective.keySet().stream()
                .anyMatch(key -> "dlqproducerproperties".equals(normalize(head(key)))
                        && (key.endsWith(".value.serializer") || key.endsWith(".key.serializer")));
        if (!hasSerializer) {
            throw new IllegalStateException(
                    ("Consumer '%s' enables a DLQ with content-type=native but its DLQ producer has no "
                            + "serializer. The payload is no longer byte[], so the binder refuses to publish "
                            + "and the record is dropped after the retries instead of being preserved. Set "
                            + "kafka-dr.consumers.%s.properties.dlq-producer-properties.configuration."
                            + "value.serializer (and key.serializer if the key is not byte[] either).")
                            .formatted(consumer.getName(), consumer.getName()));
        }
    }

    private static String findByNormalizedHead(Map<String, String> properties, String normalizedKey) {
        return properties.entrySet().stream()
                .filter(e -> normalizedKey.equals(normalize(e.getKey())))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private static String head(String key) {
        int dot = key.indexOf('.');
        return dot < 0 ? key : key.substring(0, dot);
    }

    private static String normalize(String key) {
        return key.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
    }

    /**
     * In record mode the container filters out records the deserializer could not read
     * before the listener runs. It does not do that in batch mode, so they reach the
     * handler chain and are reported as conversion failures instead.
     */
    private static void checkDeserializer(KafkaClusterProperties props, ConsumerConfig consumer) {
        Map<String, String> effective = props.getEffectiveConsumerProperties(consumer);
        boolean errorHandling = effective.values().stream()
                .anyMatch(v -> v != null && v.contains("ErrorHandlingDeserializer"));
        if (errorHandling) {
            log.warn("[{}] ErrorHandlingDeserializer behaves differently in batch mode: unreadable records "
                            + "are not filtered out before the listener runs. They surface as conversion "
                            + "failures at their position in the batch.", consumer.getName());
        }
    }
}
