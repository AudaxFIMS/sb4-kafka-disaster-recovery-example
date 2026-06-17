package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResilientProducerTest {

    private StreamBridge streamBridge;
    private ActiveClusterManager clusterManager;
    private KafkaClusterProperties properties;

    @BeforeEach
    void setup() {
        streamBridge = mock(StreamBridge.class);
        clusterManager = mock(ActiveClusterManager.class);
        properties = new KafkaClusterProperties();
        properties.getHealthCheck().setFailureThreshold(2);

        KafkaClusterProperties.ProducerConfig orderProducer = new KafkaClusterProperties.ProducerConfig();
        orderProducer.setTopic("order-events");
        properties.setProducers(Map.of("order-events-producer", orderProducer));

        when(clusterManager.getClustersByPriority()).thenReturn(List.of("primary", "secondary"));
        when(clusterManager.hasHealthyCluster()).thenReturn(true);
    }

    @Test
    void successfulSendReturnsSuccessResult() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("primary");
        assertThat(result.messageId()).isEqualTo("k-1");
    }

    @Test
    void streamBridgeFalseTriggersFailover() {
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(false);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("secondary");
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void clusterUnavailableExceptionTriggersImmediateFailover() {
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new TimeoutException("connection refused"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("secondary");
        verify(clusterManager).forceUnhealthy("primary");
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));
    }

    @Test
    void serializationErrorReturnsFailureWithoutFailover() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new SerializationException("bad schema"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isEqualTo("primary");
        verify(clusterManager, times(0)).forceUnhealthy(anyString());
    }

    @Test
    void retriesExhaustedTriggersFailover() {
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new RuntimeException("generic error"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("secondary");
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void allClustersFailedReturnsFailureResult() {
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class))).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        verify(clusterManager).forceUnhealthy("primary");
        verify(clusterManager).forceUnhealthy("secondary");
    }

    @Test
    void stuckActiveClusterAfterFailoverBreaksLoop() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void noHealthyClusterSkipsSendAndFailsFast() {
        // Reproduces "no active cluster at startup": a send must not reach
        // streamBridge (and thus KafkaTopicProvisioner) when nothing is healthy.
        when(clusterManager.hasHealthyCluster()).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        assertThat(result.messageId()).isEqualTo("k-1");
        verify(streamBridge, times(0)).send(anyString(), anyString(), any(Message.class));
        verify(clusterManager, times(0)).getActiveCluster();
    }

    @Test
    void unknownTopicFailsFastEvenWithNoHealthyCluster() {
        when(clusterManager.hasHealthyCluster()).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> producer.send("unconfigured-topic", "payload", "k-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No producer configured for topic");
    }

    @Test
    void messageIdNullGeneratesUuidKey() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", null);

        assertThat(result.messageId()).isNotBlank();
        assertThat(result.messageId()).hasSize(36);
    }

    @Test
    void customHeadersArePropagated() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenAnswer(inv -> {
                    Message<?> msg = inv.getArgument(2);
                    assertThat(msg.getHeaders())
                            .containsEntry("correlation-id", "corr-123")
                            .containsEntry(KafkaHeaders.KEY, "k-1");
                    return true;
                });

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        producer.send("order-events", "payload", "k-1", Map.of("correlation-id", "corr-123"));

        verify(streamBridge, atLeastOnce()).send(anyString(), eq("primary"), any(Message.class));
    }

    @Test
    void preBuiltMessageReusesExistingKafkaKey() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        Message<String> message = MessageBuilder.withPayload("payload")
                .setHeader(KafkaHeaders.KEY, "pre-built-key")
                .build();

        ResilientProducer.SendResult result = producer.send("order-events", message);

        assertThat(result.messageId()).isEqualTo("pre-built-key");
    }

    @Test
    void preBuiltMessageWithoutKeyGetsUuid() {
        when(clusterManager.getActiveCluster()).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        Message<String> message = MessageBuilder.withPayload("payload").build();

        ResilientProducer.SendResult result = producer.send("order-events", message);

        assertThat(result.messageId()).hasSize(36);
    }
}
