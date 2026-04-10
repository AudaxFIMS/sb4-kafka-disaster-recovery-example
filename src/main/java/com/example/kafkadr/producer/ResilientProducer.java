package com.example.kafkadr.producer;

import com.example.kafkadr.config.KafkaClusterProperties;
import com.example.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Component
public class ResilientProducer {

    private static final Logger log = LoggerFactory.getLogger(ResilientProducer.class);

    private final StreamBridge streamBridge;
    private final ActiveClusterManager clusterManager;
    private final int maxRetries;

    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                             KafkaClusterProperties properties) {
        this.streamBridge = streamBridge;
        this.clusterManager = clusterManager;
        this.maxRetries = properties.getHealthCheck().getFailureThreshold();
    }

    /**
     * Sends a message with automatic failover across clusters.
     *
     * @param topic     destination topic
     * @param payload   any payload type — String, POJO, byte[], Map, etc.
     * @param messageId optional idempotency key (generated if null)
     */
    public SendResult send(String topic, Object payload, String messageId) {
        String id = (messageId != null) ? messageId : UUID.randomUUID().toString();
        Set<String> triedClusters = new HashSet<>();

        while (triedClusters.size() < clusterManager.getClustersByPriority().size()) {
            String cluster = clusterManager.getActiveCluster();

            if (triedClusters.contains(cluster)) {
                break;
            }

            try {
                SendOutcome outcome = trySendWithRetries(topic, cluster, payload, id);
                switch (outcome) {
                    case SUCCESS:
                        return new SendResult(true, cluster, id);
                    case CLUSTER_UNAVAILABLE:
                        triedClusters.add(cluster);
                        log.warn("Cluster '{}' unavailable, forcing failover", cluster);
                        clusterManager.forceUnhealthy(cluster);
                        break;
                    case RETRIES_EXHAUSTED:
                        triedClusters.add(cluster);
                        log.warn("All {} retries exhausted for cluster '{}', forcing failover", maxRetries, cluster);
                        clusterManager.forceUnhealthy(cluster);
                        break;
                }
            } catch (SerializationException e) {
                log.warn("Serialization error, skipping publish: topic={}, messageId={}, error={}",
                        topic, id, e.getMessage());
                return new SendResult(false, cluster, id);
            }
        }

        log.error("All {} clusters unavailable. topic={}, messageId={}",
                triedClusters.size(), topic, id);
        return new SendResult(false, null, id);
    }

    private SendOutcome trySendWithRetries(String topic, String cluster, Object payload, String messageId) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Message<?> message = MessageBuilder.withPayload(payload)
                        .setHeader("message-id", messageId)
                        .setHeader("source-cluster", cluster)
                        .setHeader("sent-at", Instant.now().toString())
                        .build();

                if (streamBridge.send(topic, cluster, message)) {
                    return SendOutcome.SUCCESS;
                }
                log.warn("StreamBridge returned false for cluster '{}'", cluster);
                return SendOutcome.CLUSTER_UNAVAILABLE;
            } catch (Exception e) {
                if (isSerializationError(e)) {
                    throw (e instanceof SerializationException se) ? se : new SerializationException(e.getMessage(), e);
                }
                if (isClusterUnavailable(e)) {
                    log.warn("Cluster '{}' unavailable: {} - {}", cluster, e.getClass().getSimpleName(), e.getMessage());
                    return SendOutcome.CLUSTER_UNAVAILABLE;
                }
                log.warn("Attempt {}/{} to cluster '{}' failed: {} - {}",
                        attempt, maxRetries, cluster, e.getClass().getSimpleName(), e.getMessage());
            }
        }
        return SendOutcome.RETRIES_EXHAUSTED;
    }

    private enum SendOutcome { SUCCESS, CLUSTER_UNAVAILABLE, RETRIES_EXHAUSTED }

    private boolean isSerializationError(Throwable e) {
        while (e != null) {
            if (e instanceof SerializationException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private boolean isClusterUnavailable(Throwable e) {
        while (e != null) {
            if (e instanceof TimeoutException
                    || e instanceof NetworkException
                    || e instanceof DisconnectException
                    || e instanceof BrokerNotAvailableException
                    || e instanceof NotLeaderOrFollowerException
                    || e instanceof java.net.ConnectException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    public record SendResult(boolean success, String cluster, String messageId) {}
}
