package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.producer.ResilientProducer.BatchSendResult;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResilientProducerBatchTest {

    private StreamBridge streamBridge;
    private ActiveClusterManager clusterManager;
    private KafkaClusterProperties properties;

    @BeforeEach
    void setup() {
        streamBridge = mock(StreamBridge.class);
        clusterManager = mock(ActiveClusterManager.class);
        properties = new KafkaClusterProperties();
        properties.getHealthCheck().setFailureThreshold(2);

        KafkaClusterProperties.ProducerConfig producer = new KafkaClusterProperties.ProducerConfig();
        producer.setTopic("order-events");
        properties.setProducers(Map.of("order-events-producer", producer));

        when(clusterManager.getClustersByPriority()).thenReturn(List.of("primary", "secondary"));
        when(clusterManager.hasHealthyCluster()).thenReturn(true);
        when(clusterManager.getActiveCluster()).thenReturn("primary");
    }

    @Test
    void everyMessageIsSentToTheActiveCluster() {
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        BatchSendResult result = producer().sendBatch("order-events", batch(3));

        assertThat(result.allSent()).isTrue();
        assertThat(result.sent()).isEqualTo(3);
        assertThat(result.clusters()).containsExactly("primary");
        verify(streamBridge, org.mockito.Mockito.times(3))
                .send(anyString(), eq("primary"), any(Message.class));
    }

    @Test
    void missingKeyHeaderIsGeneratedPerMessage() {
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);
        List<Message<?>> messages = List.of(
                MessageBuilder.withPayload("a").build(),
                MessageBuilder.withPayload("b").build());

        BatchSendResult result = producer().sendBatch("order-events", messages);

        ArgumentCaptor<Message<?>> captor = ArgumentCaptor.captor();
        verify(streamBridge, org.mockito.Mockito.times(2))
                .send(anyString(), eq("primary"), captor.capture());
        assertThat(captor.getAllValues())
                .allSatisfy(m -> assertThat(m.getHeaders()).containsKey(KafkaHeaders.KEY));
        assertThat(result.results()).extracting(ResilientProducer.SendResult::messageId)
                .doesNotContainNull();
    }

    @Test
    void onlyTheUnsentTailMovesToTheNextCluster() {
        // Two messages land on primary, then it dies.
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenReturn(true, true)
                .thenThrow(new TimeoutException("primary gone"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");

        BatchSendResult result = producer().sendBatch("order-events", batch(5));

        assertThat(result.allSent()).isTrue();
        assertThat(result.clusters()).containsExactly("primary", "secondary");
        // Resending the whole batch would duplicate the two primary already acknowledged.
        verify(streamBridge, org.mockito.Mockito.times(3))
                .send(anyString(), eq("secondary"), any(Message.class));
        assertThat(result.results().get(0).cluster()).isEqualTo("primary");
        assertThat(result.results().get(1).cluster()).isEqualTo("primary");
        assertThat(result.results().get(2).cluster()).isEqualTo("secondary");
    }

    @Test
    void aDeadClusterIsAbandonedAfterTheFirstMessageNotAfterEveryMessage() {
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new TimeoutException("primary gone"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");

        producer().sendBatch("order-events", batch(50));

        // One message proves the cluster is gone; retrying the ladder for the other 49
        // would be 49 x maxRetries doomed attempts before the failover.
        verify(streamBridge, org.mockito.Mockito.times(1))
                .send(anyString(), eq("primary"), any(Message.class));
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void serializationErrorFailsOneMessageAndKeepsTheClusterAndBatchGoing() {
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenReturn(true)
                .thenThrow(new SerializationException("bad payload"))
                .thenReturn(true);

        BatchSendResult result = producer().sendBatch("order-events", batch(3));

        // A bad payload is this message's problem: failing over would reproduce it.
        assertThat(result.sent()).isEqualTo(2);
        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0).cluster()).isEqualTo("primary");
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void allClustersDownFailsEveryMessageWithoutSending() {
        when(clusterManager.hasHealthyCluster()).thenReturn(false);

        BatchSendResult result = producer().sendBatch("order-events", batch(4));

        assertThat(result.sent()).isZero();
        assertThat(result.failed()).isEqualTo(4);
        assertThat(result.clusters()).isEmpty();
        verify(streamBridge, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void exhaustingEveryClusterLeavesTheRemainderFailed() {
        when(streamBridge.send(anyString(), anyString(), any(Message.class)))
                .thenReturn(true)
                .thenThrow(new TimeoutException("gone"));
        when(clusterManager.getActiveCluster()).thenReturn("primary", "secondary");

        BatchSendResult result = producer().sendBatch("order-events", batch(4));

        assertThat(result.sent()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(3);
        assertThat(result.failures()).allSatisfy(r -> assertThat(r.cluster()).isNull());
    }

    @Test
    void emptyBatchSendsNothing() {
        BatchSendResult result = producer().sendBatch("order-events", List.of());

        assertThat(result.size()).isZero();
        assertThat(result.allSent()).isTrue();
        verify(streamBridge, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void unconfiguredTopicFailsBeforeTouchingTheCluster() {
        assertThatThrownBy(() -> producer().sendBatch("unknown-topic", batch(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kafka-dr.producers");

        verify(clusterManager, never()).hasHealthyCluster();
    }

    @Test
    void resultsKeepInputOrder() {
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        BatchSendResult result = producer().sendBatch("order-events", batch(4));

        assertThat(result.results()).extracting(ResilientProducer.SendResult::messageId)
                .containsExactly("k-0", "k-1", "k-2", "k-3");
    }

    private ResilientProducer producer() {
        return new ResilientProducer(streamBridge, clusterManager, properties);
    }

    private static List<Message<?>> batch(int size) {
        List<Message<?>> messages = new ArrayList<>(size);
        IntStream.range(0, size).forEach(i -> messages.add(MessageBuilder.withPayload("p" + i)
                .setHeader(KafkaHeaders.KEY, "k-" + i)
                .build()));
        return messages;
    }
}
