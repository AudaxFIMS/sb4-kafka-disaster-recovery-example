package dev.semeshin.kafkadr.config;

import dev.semeshin.kafkadr.config.KafkaClusterProperties.BatchConfig;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.ConsumerConfig;
import dev.semeshin.kafkadr.consumer.AckPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(OutputCaptureExtension.class)
class ConsumerConfigValidatorTest {

    @Test
    void duplicateTopicAndGroupIsRejected() {
        KafkaClusterProperties props = props(
                consumer("orders-main", "orders", "shared-group"),
                consumer("orders-audit", "orders", "shared-group"));

        // A ListenerContainerCustomizer only sees destination and group, so per-consumer
        // container settings would land on whichever of the two it resolved first.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders-main")
                .hasMessageContaining("orders-audit")
                .hasMessageContaining("different groups");
    }

    @Test
    void sameTopicWithDifferentGroupsIsFine() {
        KafkaClusterProperties props = props(
                consumer("orders-main", "orders", "processor"),
                consumer("orders-audit", "orders", "auditor"));

        assertThatCode(() -> ConsumerConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void standardModeWithIdempotencyIsRejected() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.getBatch().setMode(BatchConfig.Mode.STANDARD);

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(consumer)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("idempotency-enabled=false");
    }

    @Test
    void standardModeIsAcceptedOnceDeduplicationIsExplicitlyOff() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.getBatch().setMode(BatchConfig.Mode.STANDARD);
        consumer.setIdempotencyEnabled(false);

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void manualAckWithFailBatchIsRejected() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));

        // Partial acknowledgment needs MANUAL_IMMEDIATE; without it the successful prefix
        // cannot be committed and every failure reprocesses the whole batch.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(consumer)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("MANUAL_IMMEDIATE");
    }

    @Test
    void manualImmediateWithFailBatchIsFine() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.setProperties(Map.of("ack-mode", "MANUAL_IMMEDIATE"));

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void manualAckWithSkipFailedIsFine() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.getBatch().setErrorPolicy(BatchConfig.ErrorPolicy.SKIP_FAILED);
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void scheduledAndRecordAckModesAreAcceptedWithAWarning() {
        for (String mode : new String[] {"RECORD", "TIME", "COUNT", "COUNT_TIME"}) {
            ConsumerConfig consumer = consumer("orders", "orders", "g");
            consumer.getBatch().setEnabled(true);
            consumer.setProperties(Map.of("ack-mode", mode));

            assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer)))
                    .as("ack-mode=%s", mode).doesNotThrowAnyException();
        }
    }

    @Test
    void unknownAckModeIsRejectedWithTheOffendingValue() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.getBatch().setEnabled(true);
        consumer.setProperties(Map.of("ack-mode", "SOMETIMES"));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(consumer)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SOMETIMES");
    }

    @Test
    void batchSettingsAreIgnoredWhenBatchingIsOff() {
        ConsumerConfig consumer = consumer("orders", "orders", "g");
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));

        // Without batching there is no partial-commit problem to guard against.
        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void nativeDlqWithoutASerializerIsRejected() {
        ConsumerConfig consumer = consumer("payments", "payment-events", "g");
        consumer.setContentType("native");
        consumer.setProperties(Map.of("enable-dlq", "true", "dlq-name", "payment-events-dlq"));

        // The binder refuses to publish a non-byte[] payload without a serializer, so the
        // record the DLQ existed to preserve is dropped after the retries instead.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(consumer)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dlq-producer-properties.configuration.value.serializer");
    }

    @Test
    void nativeDlqWithASerializerIsAccepted() {
        ConsumerConfig consumer = consumer("payments", "payment-events", "g");
        consumer.setContentType("native");
        consumer.setProperties(Map.of(
                "enable-dlq", "true",
                "dlq-producer-properties", Map.of("configuration", Map.of(
                        "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"))));

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void dlqWithoutNativeDecodingNeedsNoSerializer() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.setContentType("json");
        consumer.setProperties(Map.of("enable-dlq", "true"));

        // Without native decoding the payload is still byte[] and the binder's check
        // never runs.
        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void nativeConsumerWithoutADlqIsUnaffected() {
        ConsumerConfig consumer = consumer("payments", "payment-events", "g");
        consumer.setContentType("native");

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void starterOwnedAcknowledgmentIsRejectedForBatchingConsumers() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.getBatch().setEnabled(true);
        consumer.getAck().setOwner(AckPolicy.Owner.STARTER);
        consumer.setProperties(Map.of("ack-mode", "MANUAL_IMMEDIATE"));

        // In split mode the starter already computes the commit point, and in standard mode
        // the handler owns the envelope. Ownership is only a choice on the record path.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(consumer)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ack.owner=starter")
                .hasMessageContaining("batching enabled");
    }

    @Test
    void starterOwnedAcknowledgmentIsAcceptedOnTheRecordPath() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.getAck().setOwner(AckPolicy.Owner.STARTER);
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void starterOwnedAcknowledgmentWithoutAManualAckModeIsOnlyWarnedAbout() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.getAck().setOwner(AckPolicy.Owner.STARTER);

        // Nothing is lost: the container still commits once the listener returns.
        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void manualAckModeInRecordModeIsAcceptedWithAWarning() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.setProperties(Map.of("ack-mode", "MANUAL_IMMEDIATE"));

        // The handler owning the commit is legitimate; whether it actually acknowledges is
        // reported by IdempotentConsumer at runtime.
        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void nonPositiveAckCountAndAckTimeAreRejected() {
        ConsumerConfig withCount = consumer("orders", "order-events", "g");
        withCount.getAck().setCount(0);

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(withCount)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ack.count=0");

        ConsumerConfig withTime = consumer("orders", "order-events", "g");
        withTime.getAck().setTime(0L);

        // spring-kafka asserts this inside the binder child context, which on a standby
        // cluster is only built at failover.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props(withTime)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ack.time=0");
    }

    @Test
    void containerAckSettingsThatDoNotApplyAreOnlyWarnedAbout() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.getAck().setCount(100);
        consumer.getAck().setTime(5000L);
        consumer.getAck().setAsyncAcks(true);
        consumer.setProperties(Map.of("ack-mode", "BATCH"));

        assertThatCode(() -> ConsumerConfigValidator.validate(props(consumer))).doesNotThrowAnyException();
    }

    @Test
    void asyncAcksWithSeekByTimestampIsAcceptedWithAWarning() {
        ConsumerConfig consumer = consumer("orders", "order-events", "g");
        consumer.getAck().setAsyncAcks(true);
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));
        KafkaClusterProperties props = props(consumer);
        props.getFailover().setSeekByTimestamp(true);

        // The acknowledgment arrives after the handler returns, so the watermark can never
        // follow it and seek-by-timestamp silently degrades to committed offsets.
        assertThatCode(() -> ConsumerConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void sameTopicAndGroupInDifferentClusterGroupsIsFine() {
        KafkaClusterProperties props = twoClusterGroups(
                inGroup(consumer("orders-core", "orders", "shared-group"), "core"),
                inGroup(consumer("orders-analytics", "orders", "shared-group"), "analytics"));

        // Two Kafkas: the customizer tells the containers apart by binding name.
        assertThatCode(() -> ConsumerConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void sameTopicAndGroupWithinOneClusterGroupIsStillRejected() {
        KafkaClusterProperties props = twoClusterGroups(
                inGroup(consumer("orders-main", "orders", "shared-group"), "core"),
                inGroup(consumer("orders-audit", "orders", "shared-group"), "core"));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders-audit");
    }

    @Test
    void seekByTimestampWarningFollowsTheConsumerClusterGroup(CapturedOutput output) {
        KafkaClusterProperties props = twoClusterGroups(
                inGroup(asyncManual(consumer("orders", "orders", "g1")), "core"),
                inGroup(asyncManual(consumer("scores", "scores", "g2")), "analytics"));
        props.getClusterGroups().get("analytics").getFailover().setSeekByTimestamp(true);

        ConsumerConfigValidator.validate(props);

        assertThat(output).contains("[scores] ack.async-acks with failover.seek-by-timestamp")
                .doesNotContain("[orders] ack.async-acks with failover.seek-by-timestamp");
    }

    // --- depends-on ----------------------------------------------------------------------

    @Test
    void dependsOnWithAManualAckModeIsAccepted() {
        KafkaClusterProperties props = twoClusterGroups(
                dependsOnAnalytics(withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL")));

        assertThatCode(() -> ConsumerConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void dependsOnWithoutManualAcknowledgmentIsRejected() {
        KafkaClusterProperties props = twoClusterGroups(
                dependsOnAnalytics(inGroup(consumer("bridge", "orders", "g1"), "core")));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("needs ack-mode=MANUAL or MANUAL_IMMEDIATE")
                .hasMessageContaining("BATCH (container default)")
                .hasMessageContaining("kafka-dr.consumers.bridge.properties.ack-mode=MANUAL");
    }

    @Test
    void dependsOnInSplitModeNeedsManualImmediate() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL");
        bridge.getBatch().setEnabled(true);
        KafkaClusterProperties props = twoClusterGroups(dependsOnAnalytics(bridge));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("batch.mode=split, which needs ack-mode=MANUAL_IMMEDIATE");
    }

    @Test
    void dependsOnInSplitModeRefusesSkipFailed() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL_IMMEDIATE");
        bridge.getBatch().setEnabled(true);
        bridge.getBatch().setErrorPolicy(BatchConfig.ErrorPolicy.SKIP_FAILED);
        KafkaClusterProperties props = twoClusterGroups(dependsOnAnalytics(bridge));

        // The record that failed because the dependency went down would be skipped and committed.
        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("error-policy=skip-failed");

        bridge.getBatch().setErrorPolicy(BatchConfig.ErrorPolicy.FAIL_BATCH);
        ConsumerConfigValidator.validate(props);
    }

    @Test
    void dependsOnMaxHoldMustBePositive() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL");
        bridge.setDependsOnMaxHoldMs(0);
        KafkaClusterProperties props = twoClusterGroups(dependsOnAnalytics(bridge));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("depends-on-max-hold-ms=0");
    }

    @Test
    void dependsOnWithAsyncAcksIsRejected() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL");
        bridge.getAck().setAsyncAcks(true);
        KafkaClusterProperties props = twoClusterGroups(dependsOnAnalytics(bridge));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("out-of-order commits");
    }

    @Test
    void dependsOnAnUnknownGroupIsRejected() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL");
        bridge.setDependsOn(List.of("billing"));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(twoClusterGroups(bridge)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("depends on cluster group 'billing', which is not configured")
                .hasMessageContaining("[core, analytics]");
    }

    @Test
    void dependsOnTheOwnGroupIsRejected() {
        ConsumerConfig bridge = withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL");
        bridge.setDependsOn(List.of("core"));

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(twoClusterGroups(bridge)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("depends on its own cluster group 'core'");
    }

    @Test
    void nonPositiveNackIntervalIsRejected() {
        ConsumerConfig bridge = dependsOnAnalytics(
                withAckMode(inGroup(consumer("bridge", "orders", "g1"), "core"), "MANUAL"));
        bridge.setDependsOnNackIntervalMs(0);

        assertThatThrownBy(() -> ConsumerConfigValidator.validate(twoClusterGroups(bridge)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("depends-on-nack-interval-ms=0");
    }

    private static ConsumerConfig dependsOnAnalytics(ConsumerConfig consumer) {
        consumer.setDependsOn(List.of("analytics"));
        return consumer;
    }

    private static ConsumerConfig withAckMode(ConsumerConfig consumer, String ackMode) {
        consumer.setProperties(Map.of("ack-mode", ackMode));
        return consumer;
    }

    private static ConsumerConfig asyncManual(ConsumerConfig consumer) {
        consumer.setProperties(Map.of("ack-mode", "MANUAL"));
        consumer.getAck().setAsyncAcks(true);
        return consumer;
    }

    private static ConsumerConfig inGroup(ConsumerConfig consumer, String clusterGroup) {
        consumer.setClusterGroup(clusterGroup);
        return consumer;
    }

    private static KafkaClusterProperties twoClusterGroups(ConsumerConfig... consumers) {
        KafkaClusterProperties properties = props(consumers);
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", clusterGroup("primary", "core-a:9092"));
        groups.put("analytics", clusterGroup("dc1", "an-a:9092"));
        properties.setClusterGroups(groups);
        return properties;
    }

    private static KafkaClusterProperties.ClusterGroupConfig clusterGroup(String cluster, String brokers) {
        KafkaClusterProperties.ClusterConfig cfg = new KafkaClusterProperties.ClusterConfig();
        cfg.setBootstrapServers(brokers);
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        group.setClusters(Map.of(cluster, cfg));
        return group;
    }

    private static KafkaClusterProperties props(ConsumerConfig... consumers) {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        Map<String, ConsumerConfig> map = new LinkedHashMap<>();
        for (ConsumerConfig consumer : consumers) {
            map.put(consumer.getName(), consumer);
        }
        properties.setConsumers(map);
        return properties;
    }

    private static ConsumerConfig consumer(String name, String topic, String group) {
        ConsumerConfig consumer = new ConsumerConfig();
        consumer.setName(name);
        consumer.setTopic(topic);
        consumer.setGroup(group);
        return consumer;
    }
}
