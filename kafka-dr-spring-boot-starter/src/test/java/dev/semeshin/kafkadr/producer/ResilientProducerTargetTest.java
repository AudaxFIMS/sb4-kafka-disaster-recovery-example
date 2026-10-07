package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.producer.ResilientProducer.BatchSendResult;
import dev.semeshin.kafkadr.producer.ResilientProducer.Failure;
import dev.semeshin.kafkadr.producer.ResilientProducer.SendResult;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Producer addressing by name, and the results that say where a send went and why it failed.
 * Topic {@code events} exists in both cluster groups — the case only {@code to(name)} can address.
 */
class ResilientProducerTargetTest {

    private StreamBridge streamBridge;
    private ActiveClusterManager clusterManager;
    private KafkaClusterProperties properties;

    @BeforeEach
    void setup() {
        streamBridge = mock(StreamBridge.class);
        clusterManager = mock(ActiveClusterManager.class);
        properties = new KafkaClusterProperties();
        Map<String, KafkaClusterProperties.ClusterGroupConfig> groups = new LinkedHashMap<>();
        groups.put("core", group("primary", "core-a:9092"));
        groups.put("analytics", group("dc1", "an-a:9092"));
        properties.setClusterGroups(groups);
        Map<String, KafkaClusterProperties.ProducerConfig> producers = new LinkedHashMap<>();
        producers.put("core-events", producer("events", "core"));
        producers.put("analytics-events", producer("events", "analytics"));
        producers.put("orders", producer("orders", "core"));
        properties.setProducers(producers);

        for (String group : List.of("core", "analytics")) {
            String cluster = group.equals("core") ? "core-primary" : "analytics-dc1";
            when(clusterManager.hasHealthyCluster(group)).thenReturn(true);
            when(clusterManager.getClustersByPriority(group)).thenReturn(List.of(cluster));
            when(clusterManager.getActiveCluster(group)).thenReturn(cluster);
            when(clusterManager.getHealthStatuses(group)).thenReturn(Map.of(cluster, true));
        }
        when(streamBridge.send(anyString(), anyString(), any(Message.class))).thenReturn(true);
    }

    // --- addressing --------------------------------------------------------------------

    @Test
    void toAddressesTheProducerByNameEvenWhenItsTopicExistsInSeveralGroups() {
        ResilientProducer producer = newProducer();

        SendResult core = producer.to("core-events").send("payload", "k1");
        SendResult analytics = producer.to("analytics-events").send("payload", "k2");

        assertThat(core.cluster()).isEqualTo("core-primary");
        assertThat(core.group()).isEqualTo("core");
        assertThat(analytics.cluster()).isEqualTo("analytics-dc1");
        assertThat(analytics.group()).isEqualTo("analytics");
        // Each producer has its own output binding, named after the producer.
        verify(streamBridge).send(eq("coreEvents"), eq("core-primary"), any(Message.class));
        verify(streamBridge).send(eq("analyticsEvents"), eq("analytics-dc1"), any(Message.class));
    }

    @Test
    void targetExposesWhatItAddresses() {
        ResilientProducer.Target target = newProducer().to("analytics-events");

        assertThat(target.producer()).isEqualTo("analytics-events");
        assertThat(target.topic()).isEqualTo("events");
        assertThat(target.group()).isEqualTo("analytics");
    }

    @Test
    void everySendShapeIsAvailableOnTheTarget() {
        ResilientProducer.Target target = newProducer().to("orders");

        assertThat(target.send(MessageBuilder.withPayload("p").build()).success()).isTrue();
        assertThat(target.send("p", "k", Map.of("h", "v")).success()).isTrue();
        BatchSendResult batch = target.sendBatch(List.of(
                MessageBuilder.withPayload("a").build(), MessageBuilder.withPayload("b").build()));
        assertThat(batch.allSent()).isTrue();
        assertThat(batch.results()).extracting(SendResult::group).containsOnly("core");
    }

    @Test
    void topicAddressingStillWorksWhileTheTopicIsUnambiguous() {
        SendResult result = newProducer().send("orders", "payload", "k1");

        assertThat(result.success()).isTrue();
        assertThat(result.group()).isEqualTo("core");
    }

    @Test
    void topicAddressingRefusesATopicThatExistsInSeveralGroups() {
        ResilientProducer producer = newProducer();

        assertThatThrownBy(() -> producer.send("events", "payload", "k1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("core-events (core)")
                .hasMessageContaining("analytics-events (analytics)")
                .hasMessageContaining("to(\"<producer>\")");
        assertThatThrownBy(() -> producer.sendBatch("events", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(streamBridge, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void unknownProducerNameIsRejectedWithTheKnownNames() {
        assertThatThrownBy(() -> newProducer().to("billing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No producer named 'billing'")
                .hasMessageContaining("core-events");
    }

    @Test
    void twoProducersForOneTopicInOneGroupAreRejected() {
        properties.getProducers().put("orders-again", producer("orders", "core"));

        assertThatThrownBy(this::newProducer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'orders' and 'orders-again' both write topic 'orders' in cluster group 'core'");
    }

    // --- failures -----------------------------------------------------------------------

    @Test
    void orThrowReturnsTheResultOfASuccessfulSend() {
        SendResult result = newProducer().to("orders").send("payload", "k1");

        assertThat(result.orThrow()).isSameAs(result);
        assertThat(result.failure()).isNull();
    }

    @Test
    void groupWithoutHealthyClusterIsAGroupFailure() {
        when(clusterManager.hasHealthyCluster("analytics")).thenReturn(false);

        SendResult result = newProducer().to("analytics-events").send("payload", "k1");

        assertThat(result.failure()).isEqualTo(Failure.NO_HEALTHY_CLUSTER);
        assertThatThrownBy(result::orThrow)
                .isInstanceOfSatisfying(ClusterGroupUnavailableException.class, e -> {
                    assertThat(e.getGroup()).isEqualTo("analytics");
                    assertThat(e.getMessageId()).isEqualTo("k1");
                    assertThat(e.getFailure()).isEqualTo(Failure.NO_HEALTHY_CLUSTER);
                });
    }

    @Test
    void everyClusterFailingIsAGroupFailure() {
        when(streamBridge.send(anyString(), eq("analytics-dc1"), any(Message.class))).thenReturn(false);

        SendResult result = newProducer().to("analytics-events").send("payload", "k1");

        assertThat(result.failure()).isEqualTo(Failure.ALL_CLUSTERS_FAILED);
        assertThatThrownBy(result::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
    }

    @Test
    void serializationErrorIsAMessageFailureNotAGroupFailure() {
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new SerializationException("bad schema"));

        SendResult result = newProducer().to("orders").send("payload", "k1");

        assertThat(result.failure()).isEqualTo(Failure.SERIALIZATION);
        assertThat(Failure.SERIALIZATION.groupUnavailable()).isFalse();
        assertThatThrownBy(result::orThrow)
                .isInstanceOf(SendFailedException.class)
                .isNotInstanceOf(ClusterGroupUnavailableException.class)
                .hasMessageContaining("SERIALIZATION");
    }

    @Test
    void recordTheBrokerRefusesIsAMessageFailureWithoutFailover() {
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new RuntimeException("send failed",
                        new org.apache.kafka.common.errors.RecordTooLargeException("too large")));

        SendResult result = newProducer().to("orders").send("payload", "k1");

        // Another cluster would refuse it the same way: failing over would only take the next
        // one down, and a bridge holding it back as a "dependency outage" would loop forever.
        assertThat(result.failure()).isEqualTo(Failure.REJECTED);
        assertThat(Failure.REJECTED.groupUnavailable()).isFalse();
        verify(clusterManager, never()).forceUnhealthy(anyString());
        assertThatThrownBy(result::orThrow)
                .isInstanceOf(SendFailedException.class)
                .isNotInstanceOf(ClusterGroupUnavailableException.class);
    }

    @Test
    void missingAclEverywhereIsHeldNotLost() {
        when(streamBridge.send(anyString(), eq("analytics-dc1"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("no ACL for events"));

        SendResult result = newProducer().to("analytics-events").send("payload", "k1");

        // ACLs are per cluster and deployed by hand: no proof against the message. A bridge holds
        // the record back — for at most depends-on-max-hold-ms while the group is up.
        assertThat(result.failure()).isEqualTo(Failure.ALL_CLUSTERS_FAILED);
        assertThatThrownBy(result::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void standbyWithoutTheAclWhileThePrimaryIsDownIsAGroupOutage() {
        when(clusterManager.getClustersByPriority("core")).thenReturn(List.of("core-primary", "core-secondary"));
        when(clusterManager.getHealthStatuses("core")).thenReturn(Map.of("core-primary", true, "core-secondary", true));
        when(clusterManager.getActiveCluster("core")).thenReturn("core-primary", "core-secondary");
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class))).thenReturn(false);
        when(streamBridge.send(anyString(), eq("core-secondary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("ACLs are not replicated"));

        SendResult result = newProducer().to("orders").send("payload", "k1");

        // ACLs are per cluster: this is no verdict on the message, and a bridge must hold it back.
        assertThat(result.failure()).isEqualTo(Failure.ALL_CLUSTERS_FAILED);
        assertThatThrownBy(result::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
        verify(streamBridge).send(anyString(), eq("core-secondary"), any(Message.class));
        verify(clusterManager, never()).forceUnhealthy("core-secondary");
    }

    @Test
    void retriableErrorInASingleClusterGroupIsAGroupOutageNotAMessageVerdict() {
        when(streamBridge.send(anyString(), eq("analytics-dc1"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));

        SendResult result = newProducer().to("analytics-events").send("payload", "k1");

        // depends-on must hold the bridged record back, not send it to the DLQ.
        assertThat(result.failure()).isEqualTo(Failure.ALL_CLUSTERS_FAILED);
        assertThatThrownBy(result::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
        verify(clusterManager).forceUnhealthy("analytics-dc1");
    }

    @Test
    void exhaustedRetriesFailANamedGroupOverToItsStandby() {
        twoClustersInCore();
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));

        SendResult result = newProducer().to("orders").send("payload", "k1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("core-secondary");
        verify(clusterManager).forceUnhealthy("core-primary");
    }

    @Test
    void exhaustedRetriesMoveTheRestOfANamedGroupsBatchToItsStandby() {
        twoClustersInCore();
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenReturn(true)
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));

        BatchSendResult result = newProducer().to("orders").sendBatch(List.of(
                MessageBuilder.withPayload("a").setHeader(KafkaHeaders.KEY, "k1").build(),
                MessageBuilder.withPayload("b").setHeader(KafkaHeaders.KEY, "k2").build(),
                MessageBuilder.withPayload("c").setHeader(KafkaHeaders.KEY, "k3").build()));

        assertThat(result.allSent()).isTrue();
        assertThat(result.results()).extracting(SendResult::cluster)
                .containsExactly("core-primary", "core-secondary", "core-secondary");
        verify(clusterManager).forceUnhealthy("core-primary");
    }

    private void twoClustersInCore() {
        when(clusterManager.getClustersByPriority("core")).thenReturn(List.of("core-primary", "core-secondary"));
        // As the manager behaves: forcing the active cluster down elects the standby.
        java.util.concurrent.atomic.AtomicReference<String> active = new java.util.concurrent.atomic.AtomicReference<>("core-primary");
        when(clusterManager.getActiveCluster("core")).thenAnswer(inv -> active.get());
        org.mockito.Mockito.doAnswer(inv -> {
            active.set("core-secondary");
            return null;
        }).when(clusterManager).forceUnhealthy("core-primary");
        when(clusterManager.getHealthStatuses("core")).thenReturn(Map.of("core-primary", true, "core-secondary", true));
        when(streamBridge.send(anyString(), eq("core-secondary"), any(Message.class))).thenReturn(true);
    }

    @Test
    void corruptRecordIsRetriedNotRefused() {
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.CorruptRecordException("CRC mismatch in transit"))
                .thenReturn(true);

        SendResult result = newProducer().to("orders").send("payload", "k1");

        assertThat(result.success()).isTrue();
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void refusedRecordInABatchIsSkippedAndTheRestIsSent() {
        Message<?> tooLarge = MessageBuilder.withPayload("huge").setHeader("size", "huge").build();
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class))).thenAnswer(inv -> {
            Message<?> sent = inv.getArgument(2);
            if ("huge".equals(sent.getPayload())) {
                throw new org.apache.kafka.common.errors.RecordTooLargeException("too large");
            }
            return true;
        });

        BatchSendResult batch = newProducer().to("orders").sendBatch(List.of(
                MessageBuilder.withPayload("a").build(), tooLarge, MessageBuilder.withPayload("b").build()));

        assertThat(batch.sent()).isEqualTo(2);
        assertThat(batch.failures()).singleElement()
                .extracting(SendResult::failure).isEqualTo(Failure.REJECTED);
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void batchOrThrowReportsTheGroupFailureFirst() {
        // The first message is refused for what it is; the cluster goes away behind it.
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.RecordTooLargeException("too large"))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("gone"));

        BatchSendResult batch = newProducer().to("orders").sendBatch(List.of(
                MessageBuilder.withPayload("a").build(), MessageBuilder.withPayload("b").build(),
                MessageBuilder.withPayload("c").build()));

        assertThat(batch.failures()).extracting(SendResult::failure)
                .containsExactly(Failure.REJECTED, Failure.ALL_CLUSTERS_FAILED, Failure.ALL_CLUSTERS_FAILED);
        // The group failure is the one a bridge must hold the batch back for — not the first in line.
        assertThatThrownBy(batch::orThrow)
                .isInstanceOf(ClusterGroupUnavailableException.class)
                .hasMessageContaining("cluster group 'core'");
    }

    @Test
    void batchOrThrowReturnsTheResultWhenEverythingWasSent() {
        BatchSendResult batch = newProducer().to("orders").sendBatch(List.of(MessageBuilder.withPayload("a").build()));

        assertThat(batch.orThrow()).isSameAs(batch);
    }

    @Test
    void preGroupResultShapeStillConstructs() {
        SendResult ok = new SendResult(true, "primary", "k1");
        SendResult failed = new SendResult(false, null, "k2");

        assertThat(ok.group()).isNull();
        assertThat(ok.failure()).isNull();
        assertThatThrownBy(failed::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
    }

    private ResilientProducer newProducer() {
        return new ResilientProducer(streamBridge, clusterManager, properties);
    }

    private static KafkaClusterProperties.ProducerConfig producer(String topic, String clusterGroup) {
        KafkaClusterProperties.ProducerConfig producer = new KafkaClusterProperties.ProducerConfig();
        producer.setTopic(topic);
        producer.setClusterGroup(clusterGroup);
        return producer;
    }

    private static KafkaClusterProperties.ClusterGroupConfig group(String cluster, String brokers) {
        KafkaClusterProperties.ClusterConfig cfg = new KafkaClusterProperties.ClusterConfig();
        cfg.setBootstrapServers(brokers);
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        group.setClusters(Map.of(cluster, cfg));
        return group;
    }
}
