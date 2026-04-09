package com.example.kafkadr.producer;

import com.example.kafkadr.routing.ActiveClusterManager;
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

    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager) {
        this.streamBridge = streamBridge;
        this.clusterManager = clusterManager;
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

            if (trySend(topic, cluster, payload, id)) {
                return new SendResult(true, cluster, id);
            }

            triedClusters.add(cluster);
            log.warn("Send to cluster '{}' failed, forcing failover", cluster);
            clusterManager.forceUnhealthy(cluster);
        }

        log.error("All {} clusters unavailable. topic={}, messageId={}",
                triedClusters.size(), topic, id);
        return new SendResult(false, null, id);
    }

    private boolean trySend(String topic, String cluster, Object payload, String messageId) {
        try {
            Message<?> message = MessageBuilder.withPayload(payload)
                    .setHeader("message-id", messageId)
                    .setHeader("source-cluster", cluster)
                    .setHeader("sent-at", Instant.now().toString())
                    .build();

            return streamBridge.send(topic, cluster, message);
        } catch (Exception e) {
            log.warn("Send to cluster '{}' failed: {} - {}", cluster, e.getClass().getSimpleName(), e.getMessage());
            if (e.getCause() != null) {
                log.warn("  Caused by: {} - {}", e.getCause().getClass().getSimpleName(), e.getCause().getMessage());
            }
            return false;
        }
    }

    public record SendResult(boolean success, String cluster, String messageId) {}
}
