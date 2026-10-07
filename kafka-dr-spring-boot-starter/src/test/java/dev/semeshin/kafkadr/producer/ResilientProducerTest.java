package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.DisconnectException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

        when(clusterManager.getClustersByPriority("default")).thenReturn(List.of("primary", "secondary"));
        when(clusterManager.hasHealthyCluster("default")).thenReturn(true);
        when(clusterManager.getHealthStatuses("default")).thenReturn(Map.of("primary", true, "secondary", true));
    }

    @Test
    void successfulSendReturnsSuccessResult() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("primary");
        assertThat(result.messageId()).isEqualTo("k-1");
    }

    @Test
    void streamBridgeFalseTriggersFailover() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
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
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
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
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new SerializationException("bad schema"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isEqualTo("primary");
        verify(clusterManager, times(0)).forceUnhealthy(anyString());
    }

    @Test
    void retriableErrorOutlastingTheRetriesFailsOver() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isTrue();
        assertThat(result.cluster()).isEqualTo("secondary");
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void retriableBrokerErrorIsTheClustersEvenOnTheLastCluster() {
        when(clusterManager.getHealthStatuses("default")).thenReturn(Map.of("primary", true, "secondary", false));
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        // The cluster cannot take writes: a group outage, which a bridge must hold the record back for.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(clusterManager).forceUnhealthy("primary");
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void notTakenIsHeldWithoutRetryingOrFailingOver() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("no ACL"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult first = producer.send("order-events", "payload", "k-1");
        ResilientProducer.SendResult second = producer.send("order-events", "payload", "k-2");

        // A missing ACL proves nothing about the message: no message verdict, so a bridge holds
        // the record back instead of losing it. Nor about the cluster: nothing marked down, and
        // the next message gets the same answer, not a different one.
        assertThat(first.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        assertThat(second.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        assertThatThrownBy(first::orThrow).isInstanceOf(ClusterGroupUnavailableException.class);
        verify(clusterManager, never()).forceUnhealthy(anyString());
        // A definitive answer is not retried: one attempt per message.
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void notTakenByTheActiveClusterIsHeldNotWrittenToAStandby() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("ACL missing on primary"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer.SendResult result =
                new ResilientProducer(streamBridge, clusterManager, properties).send("order-events", "payload", "k-1");

        // The consumers read the primary, and replication runs from it, not to it: a record on the
        // standby would sit there unread. Held instead — and one topic's ACL fails nothing over.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void notTakenWithTheStandbyDownIsHeldWithoutMarkingAnything() {
        when(clusterManager.getHealthStatuses("default")).thenReturn(Map.of("primary", true, "secondary", false));
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("no ACL"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager, never()).forceUnhealthy(anyString());
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void schemaRegistryThatCannotBeReachedIsNoVerdictOnTheMessage() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new SerializationException("Error registering schema",
                        new java.net.ConnectException("sr-primary refused the connection")));

        ResilientProducer.SendResult result =
                new ResilientProducer(streamBridge, clusterManager, properties).send("order-events", "payload", "k-1");

        // Not SERIALIZATION — the message is fine — and not the Kafka cluster's outage either: the
        // registry's I/O is not the broker's. Held until the registry is back.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager, never()).forceUnhealthy(anyString());
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void payloadStreamBridgeCannotConvertIsASerializationFailure() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new org.springframework.messaging.converter.MessageConversionException("no converter"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.SERIALIZATION);
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void allClustersFailedReturnsFailureResult() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class))).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        verify(clusterManager).forceUnhealthy("primary");
        verify(clusterManager).forceUnhealthy("secondary");
    }

    @Test
    void allClustersFailedWithDebugStillReportsFailure() {
        // The terminal lines carry the last exception when debug is on; the failure
        // path itself must stay exactly the same.
        properties.getDebug().setEnable(true);
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("below min.insync.replicas"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        assertThat(result.messageId()).isEqualTo("k-1");
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager).forceUnhealthy("primary");
        verify(clusterManager).forceUnhealthy("secondary");
    }

    @Test
    void exceptionThatIsNotKafkasMarksNothingDown() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class)))
                .thenThrow(new IllegalArgumentException("interceptor rejected the record"));

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        // Proof of nothing: retried, then held — but no cluster goes down for it, so one bad
        // record cannot take the whole group with it.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
        verify(clusterManager, never()).forceUnhealthy(anyString());
    }

    @Test
    void streamBridgeFalseWithDebugHasNoExceptionToLog() {
        // StreamBridge returning false produces no exception: the debug branch must not
        // hand a null throwable to the logger.
        properties.getDebug().setEnable(true);
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), anyString(), any(Message.class))).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        verify(clusterManager).forceUnhealthy("primary");
        verify(clusterManager).forceUnhealthy("secondary");
    }

    @Test
    // A regression here is an endless loop, which only a separate thread can cut short.
    @org.junit.jupiter.api.Timeout(value = 5, threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    void activeClusterThatStaysActiveAfterFailingIsNotTriedAgain() {
        // The manager keeps a cluster active when it has nowhere to go: nothing is left to take it.
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(false);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager).forceUnhealthy("primary");
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));
        // Only a cluster the manager elects is written to — that is where the consumers follow.
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void activeClusterAnotherSenderMarkedDownIsNotWrittenAround() {
        when(clusterManager.getHealthStatuses("default")).thenReturn(Map.of("primary", false, "secondary", true));
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ResilientProducer.SendResult result =
                new ResilientProducer(streamBridge, clusterManager, properties).send("order-events", "payload", "k-1");

        // Still active, already marked down: the manager is about to elect another. Not sent to
        // the standby behind its back — its consumers are not there yet.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, never()).send(anyString(), anyString(), any(Message.class));
    }

    @org.junit.jupiter.params.ParameterizedTest(name = "{0}")
    @org.junit.jupiter.params.provider.MethodSource("classifiedErrors")
    void everyErrorIsClassifiedByTheTable(String name, RuntimeException error, ResilientProducer.Failure failure,
                                          boolean marksClusterDown, int attempts) {
        // Above one, so "unreachable: fail over at once" and "retried, then failed" differ.
        properties.getHealthCheck().setFailureThreshold(3);
        when(clusterManager.getHealthStatuses("default")).thenReturn(Map.of("primary", true));
        when(clusterManager.getClustersByPriority("default")).thenReturn(List.of("primary"));
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(error);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.failure()).isEqualTo(failure);
        verify(clusterManager, times(marksClusterDown ? 1 : 0)).forceUnhealthy("primary");
        verify(streamBridge, times(attempts)).send(anyString(), eq("primary"), any(Message.class));
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> classifiedErrors() {
        ResilientProducer.Failure held = ResilientProducer.Failure.ALL_CLUSTERS_FAILED;
        ResilientProducer.Failure rejected = ResilientProducer.Failure.REJECTED;
        ResilientProducer.Failure serialization = ResilientProducer.Failure.SERIALIZATION;
        return java.util.stream.Stream.of(
                // proof of the message: not retried, nothing marked
                args(new SerializationException("bad schema"), serialization, false, 1),
                // the schema itself is wrong, as Confluent 8.x reports it
                args(new SerializationException("Error registering Avro schema", new RestClientException(422)),
                        serialization, false, 1),
                args(new SerializationException("Error registering Avro schema", new RestClientException(400)),
                        serialization, false, 1),
                args(new SerializationException("Can't serialize data",
                        new JsonProcessingException("No serializer found for class Order")),
                        serialization, false, 1),
                args(new org.apache.kafka.common.errors.RecordTooLargeException("too large"), rejected, false, 1),
                args(new org.apache.kafka.common.errors.RecordBatchTooLargeException("batch too large"), rejected, false, 1),
                args(new org.apache.kafka.common.InvalidRecordException("invalid"), rejected, false, 1),
                args(new org.apache.kafka.common.errors.InvalidTimestampException("timestamp out of range"), rejected, false, 1),
                args(new org.apache.kafka.common.errors.InvalidTopicException("bad name"), rejected, false, 1),
                // proof of the cluster — unreachable: at once
                args(new TimeoutException("timeout"), held, true, 1),
                args(new org.apache.kafka.common.errors.NetworkException("network"), held, true, 1),
                args(new org.apache.kafka.common.errors.DisconnectException("disconnect"), held, true, 1),
                args(new org.apache.kafka.common.errors.BrokerNotAvailableException("broker"), held, true, 1),
                args(new org.apache.kafka.common.errors.NotLeaderOrFollowerException("leader moved"), held, true, 1),
                args(new RuntimeException(new java.net.ConnectException("refused")), held, true, 1),
                // proof of the cluster — retriable: after the retries
                args(new org.apache.kafka.common.errors.NotEnoughReplicasException("isr"), held, true, 3),
                args(new org.apache.kafka.common.errors.CorruptRecordException("crc"), held, true, 3),
                // proof of neither — a definitive answer: not retried
                args(new org.apache.kafka.common.errors.TopicAuthorizationException("acl"), held, false, 1),
                args(new org.apache.kafka.common.errors.ClusterAuthorizationException("idempotent write"), held, false, 1),
                args(new org.apache.kafka.common.errors.UnknownServerException("unknown"), held, false, 1),
                // proof of neither — the schema registry, in the shapes Confluent 8.x throws: retried,
                // and never read as the broker, though some arrive as Kafka's own TimeoutException
                args(new TimeoutException("Error retrieving Avro schema", new RestClientException(503)), held, false, 3),
                args(new TimeoutException("Error retrieving Avro schema", new RestClientException(504)), held, false, 3),
                args(new TimeoutException("Error retrieving Avro schema", new RestClientException(408)), held, false, 3),
                args(new TimeoutException("Error retrieving Avro schema", new RestClientException(500)), held, false, 3),
                args(new DisconnectException("Error retrieving Avro schema", new RestClientException(502)), held, false, 3),
                args(new TimeoutException("Error serializing Avro message",
                        new java.net.SocketTimeoutException("Read timed out")), held, false, 3),
                args(new SerializationException("Error serializing Avro message",
                        new java.net.ConnectException("Connection refused")), held, false, 3),
                args(new org.apache.kafka.common.errors.AuthorizationException("Error retrieving Avro schema",
                        new RestClientException(403)), held, false, 3),
                args(new org.apache.kafka.common.errors.AuthenticationException("Error retrieving Avro schema",
                        new RestClientException(401)), held, false, 3),
                args(new org.apache.kafka.common.errors.ThrottlingQuotaExceededException("Too many requests"), held, false, 3),
                args(new SerializationException("Error serializing Avro message", new RestClientException(429)), held, false, 3),
                args(new SerializationException("Error retrieving Avro schema", new RestClientException(404, 50005)),
                        held, false, 3),
                // this cluster's registry answers for its own state — another cluster's may differ: not retried
                args(new SerializationException("Error retrieving Avro schema", new RestClientException(404)), held, false, 1),
                args(new SerializationException("Error registering Avro schema", new RestClientException(409)), held, false, 1),
                args(new SerializationException("Error registering Avro schema", new RestClientException(422, 42205)),
                        held, false, 1),
                args(new SerializationException("Error serializing Avro message",
                        new java.io.IOException("Incompatible schema of type 'AVRO' with the latest version")),
                        held, false, 1),
                // the registry client failing in transport — whatever the IOException's type
                args(new SerializationException("Error retrieving Avro schema",
                        fromRegistryClient(new java.io.IOException("The target server failed to respond"))), held, false, 3),
                args(new SerializationException("Error retrieving Avro schema",
                        fromRegistryClient(new JsonProcessingException("Unexpected character '<'"))), held, false, 3),
                // proof of neither — anything else: retried
                args(new IllegalStateException("not Kafka's"), held, false, 3));
    }

    private static org.junit.jupiter.params.provider.Arguments args(RuntimeException error,
                                                                    ResilientProducer.Failure failure,
                                                                    boolean marksClusterDown, int attempts) {
        return org.junit.jupiter.params.provider.Arguments.of(error.getClass().getSimpleName()
                + (error.getCause() == null ? "" : " <- " + error.getCause().getClass().getSimpleName()
                + (error.getCause() instanceof RestClientException r ? " " + r.getStatus() : "")),
                error, failure, marksClusterDown, attempts);
    }

    /** As thrown inside Confluent's registry client — the origin, not the type, says it is transport. */
    private static <T extends Throwable> T fromRegistryClient(T t) {
        t.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.confluent.kafka.schemaregistry.client.rest.RestService",
                        "sendHttpRequest", "RestService.java", 330)});
        return t;
    }

    /** Stands in for Jackson 2's, an {@code IOException} that spring-kafka's JsonSerializer wraps. */
    static final class JsonProcessingException extends java.io.IOException {
        JsonProcessingException(String message) {
            super(message);
        }
    }

    /** Stands in for Confluent's, which the producer reads by name and {@code getStatus()}. */
    static final class RestClientException extends Exception {
        private final int status;
        private final int errorCode;

        RestClientException(int status) {
            this(status, status * 100);
        }

        RestClientException(int status, int errorCode) {
            super("HTTP " + status + ", error code " + errorCode);
            this.status = status;
            this.errorCode = errorCode;
        }

        public int getStatus() {
            return status;
        }

        public int getErrorCode() {
            return errorCode;
        }
    }

    @Test
    void brokerErrorAfterARegistryOutageIsStillTheBrokers() {
        properties.getHealthCheck().setFailureThreshold(3);
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class)))
                .thenThrow(new TimeoutException("Error retrieving Avro schema", new RestClientException(503)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("isr"));
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        new ResilientProducer(streamBridge, clusterManager, properties).send("order-events", "payload", "k-1");

        // The registry recovered; the broker then could not take writes — that is proof against it.
        verify(clusterManager).forceUnhealthy("primary");
    }

    @Test
    void topicMissingOnAClusterThatAnswersMarksNothingDown() {
        TimeoutException metadata = new TimeoutException("Topic order-events not present in metadata after 60000 ms.");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(metadata);

        ClusterReachability reachability = mock(ClusterReachability.class);
        when(reachability.isReachable("primary")).thenReturn(true);
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), reachability);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        // The cluster answers a metadata request: the topic is missing — or a restarted broker has
        // not loaded it yet. The cluster is fine; the message is held.
        assertThat(result.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager, never()).forceUnhealthy(anyString());
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
        verify(reachability, times(1)).isReachable("primary");
    }

    @Test
    void clusterLackingTheTopicIsNotWaitedOnForEverySend() {
        TimeoutException metadata = new TimeoutException("Topic order-events not present in metadata after 60000 ms.");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(metadata);
        java.time.Instant start = java.time.Instant.parse("2026-10-06T10:00:00Z");
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), cluster -> true);
        producer.setClock(java.time.Clock.fixed(start, java.time.ZoneOffset.UTC));

        producer.send("order-events", "payload", "k-1");
        ResilientProducer.SendResult second = producer.send("order-events", "payload", "k-2");

        // One wait for metadata, not one per send: the next fails — is held — at once.
        assertThat(second.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));

        // Someone may have created the topic since: checked again after a while.
        producer.setClock(java.time.Clock.fixed(start.plus(ResilientProducer.MISSING_TOPIC_RECHECK),
                java.time.ZoneOffset.UTC));
        producer.send("order-events", "payload", "k-3");
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void onlyOneSenderRechecksAClusterLackingTheTopic() {
        TimeoutException metadata = new TimeoutException("Topic order-events not present in metadata after 60000 ms.");
        java.time.Instant start = java.time.Instant.parse("2026-10-06T10:00:00Z");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), cluster -> true);
        producer.setClock(java.time.Clock.fixed(start, java.time.ZoneOffset.UTC));
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(metadata);
        producer.send("order-events", "payload", "k-1");
        producer.setClock(java.time.Clock.fixed(start.plus(ResilientProducer.MISSING_TOPIC_RECHECK),
                java.time.ZoneOffset.UTC));

        // The recheck waits on metadata; another sender arrives meanwhile.
        java.util.List<ResilientProducer.SendResult> meanwhile = new java.util.ArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean arrived = new java.util.concurrent.atomic.AtomicBoolean();
        org.mockito.Mockito.doAnswer(inv -> {
            // Once: a regression that let the second sender recheck too would recurse otherwise.
            if (arrived.compareAndSet(false, true)) {
                meanwhile.add(producer.send("order-events", "payload", "k-meanwhile"));
            }
            throw metadata;
        }).when(streamBridge).send(anyString(), eq("primary"), any(Message.class));
        producer.send("order-events", "payload", "k-2");

        // It failed at once rather than queueing behind the recheck.
        assertThat(meanwhile).singleElement().extracting(ResilientProducer.SendResult::failure)
                .isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, times(2)).send(anyString(), eq("primary"), any(Message.class));
        verify(streamBridge, never()).send(anyString(), eq("secondary"), any(Message.class));
    }

    @Test
    void clusterThatTakesTheTopicAgainIsUsedAgainAtOnce() {
        TimeoutException metadata = new TimeoutException("Topic order-events not present in metadata after 60000 ms.");
        java.time.Instant start = java.time.Instant.parse("2026-10-06T10:00:00Z");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(metadata).thenReturn(true);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), cluster -> true);
        producer.setClock(java.time.Clock.fixed(start, java.time.ZoneOffset.UTC));
        producer.send("order-events", "payload", "k-1");
        producer.setClock(java.time.Clock.fixed(start.plus(ResilientProducer.MISSING_TOPIC_RECHECK),
                java.time.ZoneOffset.UTC));

        // The topic was created: the recheck succeeds, and the next send needs no recheck.
        assertThat(producer.send("order-events", "payload", "k-2").cluster()).isEqualTo("primary");
        assertThat(producer.send("order-events", "payload", "k-3").cluster()).isEqualTo("primary");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"expiring", "leader", "network"})
    void probeIsOnlyForTheMetadataTimeout(String kind) {
        RuntimeException error = switch (kind) {
            case "expiring" -> new TimeoutException("Expiring 1 record(s) for order-events-0: 120000 ms has passed");
            case "leader" -> new org.apache.kafka.common.errors.NotLeaderOrFollowerException("leader moved");
            default -> new org.apache.kafka.common.errors.NetworkException("network");
        };
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(error);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        ClusterReachability reachability = mock(ClusterReachability.class);
        when(reachability.isReachable(anyString())).thenReturn(true);

        new ResilientProducer(streamBridge, clusterManager, properties, properties.topology(), reachability)
                .send("order-events", "payload", "k-1");

        // An answering cluster does not excuse these: only a missing topic looks like a dead cluster.
        verify(clusterManager).forceUnhealthy("primary");
        verify(reachability, never()).isReachable(anyString());
    }

    @Test
    void topicDeletedUnderAProducerThatKnewItMarksNothingDown() {
        // The producer kept the topic in its metadata, so it batched the record and timed it out —
        // exactly what it does for a broker that is gone.
        TimeoutException expired = new TimeoutException("Expiring 1 record(s) for order-events-0:10001 ms has passed");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(expired);
        ClusterReachability reachability = mock(ClusterReachability.class);
        when(reachability.lacksTopic("primary", "order-events")).thenReturn(true);
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), reachability);

        ResilientProducer.SendResult first = producer.send("order-events", "payload", "k-1");
        ResilientProducer.SendResult second = producer.send("order-events", "payload", "k-2");

        // The cluster answers that the topic is gone: no failover — which the health probe would only
        // undo — and the record is held, so depends-on-max-hold-ms applies.
        assertThat(first.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(clusterManager, never()).forceUnhealthy(anyString());
        // Not waited on again for the next send.
        assertThat(second.failure()).isEqualTo(ResilientProducer.Failure.ALL_CLUSTERS_FAILED);
        verify(streamBridge, times(1)).send(anyString(), eq("primary"), any(Message.class));
    }

    @Test
    void expiredRecordOnAClusterThatHasTheTopicStillFailsOver() {
        TimeoutException expired = new TimeoutException("Expiring 1 record(s) for order-events-0:10001 ms has passed");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(expired);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);
        ClusterReachability reachability = mock(ClusterReachability.class);

        ResilientProducer.SendResult result = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), reachability).send("order-events", "payload", "k-1");

        assertThat(result.cluster()).isEqualTo("secondary");
        verify(clusterManager).forceUnhealthy("primary");
        verify(reachability).lacksTopic("primary", "order-events");
    }

    @Test
    void metadataTimeoutOnAClusterThatDoesNotAnswerFailsOver() {
        TimeoutException metadata = new TimeoutException("Topic order-events not present in metadata after 60000 ms.");
        when(clusterManager.getActiveCluster("default")).thenReturn("primary", "secondary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenThrow(metadata);
        when(streamBridge.send(anyString(), eq("secondary"), any(Message.class))).thenReturn(true);

        ClusterReachability reachability = mock(ClusterReachability.class);
        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties,
                properties.topology(), reachability);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.cluster()).isEqualTo("secondary");
        verify(clusterManager).forceUnhealthy("primary");
        verify(reachability).isReachable("primary");
        // One probe: a cluster that does not answer is not asked for its topics too.
        verify(reachability, never()).lacksTopic(anyString(), anyString());
    }

    @Test
    void groupFailureThresholdIsTheProducersRetryCount() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.getHealthCheck().setFailureThreshold(1);
        KafkaClusterProperties.ClusterGroupConfig core = group("primary", "core-a:9092", "secondary", "core-b:9092");
        core.getHealthCheck().setFailureThreshold(3);
        props.setClusterGroups(Map.of("core", core));
        KafkaClusterProperties.ProducerConfig orders = new KafkaClusterProperties.ProducerConfig();
        orders.setTopic("orders");
        orders.setClusterGroup("core");
        props.setProducers(Map.of("orders-producer", orders));
        when(clusterManager.hasHealthyCluster("core")).thenReturn(true);
        when(clusterManager.getClustersByPriority("core")).thenReturn(List.of("core-primary"));
        when(clusterManager.getActiveCluster("core")).thenReturn("core-primary");
        when(clusterManager.getHealthStatuses("core")).thenReturn(Map.of("core-primary", true));
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class)))
                .thenThrow(new org.apache.kafka.common.errors.NotEnoughReplicasException("isr"));

        new ResilientProducer(streamBridge, clusterManager, props).send("orders", "o", "k1");

        verify(streamBridge, times(3)).send(anyString(), eq("core-primary"), any(Message.class));
    }

    @Test
    void noHealthyClusterSkipsSendAndFailsFast() {
        // Reproduces "no active cluster at startup": a send must not reach
        // streamBridge (and thus KafkaTopicProvisioner) when nothing is healthy.
        when(clusterManager.hasHealthyCluster("default")).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", "k-1");

        assertThat(result.success()).isFalse();
        assertThat(result.cluster()).isNull();
        assertThat(result.messageId()).isEqualTo("k-1");
        verify(streamBridge, times(0)).send(anyString(), anyString(), any(Message.class));
        verify(clusterManager, never()).getActiveCluster(anyString());
    }

    @Test
    void unknownTopicFailsFastEvenWithNoHealthyCluster() {
        when(clusterManager.hasHealthyCluster("default")).thenReturn(false);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> producer.send("unconfigured-topic", "payload", "k-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No producer configured for topic");
    }

    @Test
    void messageIdNullGeneratesUuidKey() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        ResilientProducer.SendResult result = producer.send("order-events", "payload", null);

        assertThat(result.messageId()).isNotBlank();
        assertThat(result.messageId()).hasSize(36);
    }

    @Test
    void customHeadersArePropagated() {
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
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
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
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
        when(clusterManager.getActiveCluster("default")).thenReturn("primary");
        when(streamBridge.send(anyString(), eq("primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, properties);
        Message<String> message = MessageBuilder.withPayload("payload").build();

        ResilientProducer.SendResult result = producer.send("order-events", message);

        assertThat(result.messageId()).hasSize(36);
    }

    @Test
    void eachProducerFailsOverWithinItsOwnClusterGroup() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(Map.of(
                "core", group("primary", "core-a:9092", "secondary", "core-b:9092"),
                "analytics", group("dc1", "an-a:9092", "dc2", "an-b:9092")));
        KafkaClusterProperties.ProducerConfig orders = new KafkaClusterProperties.ProducerConfig();
        orders.setTopic("orders");
        orders.setClusterGroup("core");
        KafkaClusterProperties.ProducerConfig scores = new KafkaClusterProperties.ProducerConfig();
        scores.setTopic("scores");
        scores.setClusterGroup("analytics");
        props.setProducers(Map.of("orders-producer", orders, "scores-producer", scores));

        when(clusterManager.hasHealthyCluster("core")).thenReturn(true);
        when(clusterManager.hasHealthyCluster("analytics")).thenReturn(true);
        when(clusterManager.getClustersByPriority("core")).thenReturn(List.of("core-primary", "core-secondary"));
        when(clusterManager.getClustersByPriority("analytics")).thenReturn(List.of("analytics-dc1", "analytics-dc2"));
        when(clusterManager.getActiveCluster("core")).thenReturn("core-primary");
        when(clusterManager.getActiveCluster("analytics")).thenReturn("analytics-dc1", "analytics-dc2");
        when(clusterManager.getHealthStatuses("core")).thenReturn(Map.of("core-primary", true, "core-secondary", true));
        when(clusterManager.getHealthStatuses("analytics")).thenReturn(Map.of("analytics-dc1", true, "analytics-dc2", true));
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class))).thenReturn(true);
        when(streamBridge.send(anyString(), eq("analytics-dc1"), any(Message.class))).thenReturn(false);
        when(streamBridge.send(anyString(), eq("analytics-dc2"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, props);

        assertThat(producer.send("orders", "o", "k1").cluster()).isEqualTo("core-primary");
        assertThat(producer.send("scores", "s", "k2").cluster()).isEqualTo("analytics-dc2");
        verify(clusterManager).forceUnhealthy("analytics-dc1");
        verify(clusterManager, never()).forceUnhealthy("core-primary");
    }

    @Test
    void producerOfAGroupWithoutHealthyClustersFailsFastWhileOthersSend() {
        KafkaClusterProperties props = new KafkaClusterProperties();
        props.setClusterGroups(Map.of(
                "core", group("primary", "core-a:9092", "secondary", "core-b:9092"),
                "analytics", group("dc1", "an-a:9092", "dc2", "an-b:9092")));
        KafkaClusterProperties.ProducerConfig orders = new KafkaClusterProperties.ProducerConfig();
        orders.setTopic("orders");
        orders.setClusterGroup("core");
        KafkaClusterProperties.ProducerConfig scores = new KafkaClusterProperties.ProducerConfig();
        scores.setTopic("scores");
        scores.setClusterGroup("analytics");
        props.setProducers(Map.of("orders-producer", orders, "scores-producer", scores));

        when(clusterManager.hasHealthyCluster("core")).thenReturn(true);
        when(clusterManager.hasHealthyCluster("analytics")).thenReturn(false);
        when(clusterManager.getClustersByPriority("core")).thenReturn(List.of("core-primary", "core-secondary"));
        when(clusterManager.getActiveCluster("core")).thenReturn("core-primary");
        when(clusterManager.getHealthStatuses("core")).thenReturn(Map.of("core-primary", true, "core-secondary", true));
        when(streamBridge.send(anyString(), eq("core-primary"), any(Message.class))).thenReturn(true);

        ResilientProducer producer = new ResilientProducer(streamBridge, clusterManager, props);

        assertThat(producer.send("scores", "s", "k2").success()).isFalse();
        assertThat(producer.send("orders", "o", "k1").success()).isTrue();
        verify(streamBridge, never()).send(anyString(), eq("analytics-dc1"), any(Message.class));
    }

    private static KafkaClusterProperties.ClusterGroupConfig group(String a, String brokersA, String b, String brokersB) {
        KafkaClusterProperties.ClusterGroupConfig group = new KafkaClusterProperties.ClusterGroupConfig();
        java.util.Map<String, KafkaClusterProperties.ClusterConfig> clusters = new java.util.LinkedHashMap<>();
        KafkaClusterProperties.ClusterConfig first = new KafkaClusterProperties.ClusterConfig();
        first.setBootstrapServers(brokersA);
        first.setPriority(1);
        KafkaClusterProperties.ClusterConfig second = new KafkaClusterProperties.ClusterConfig();
        second.setBootstrapServers(brokersB);
        second.setPriority(2);
        clusters.put(a, first);
        clusters.put(b, second);
        group.setClusters(clusters);
        return group;
    }
}
