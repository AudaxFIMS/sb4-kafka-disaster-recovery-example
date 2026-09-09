package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ResilientProducer {

    private static final Logger log = LoggerFactory.getLogger(ResilientProducer.class);

    private final StreamBridge streamBridge;
    private final ActiveClusterManager clusterManager;
    private final int maxRetries;
    /** Diagnostic logging switch — see {@code kafka-dr.debug.enable}. */
    private final boolean debugEnabled;
    private final Map<String, String> bindingByTopic;

    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                             KafkaClusterProperties properties) {
        this.streamBridge = streamBridge;
        this.clusterManager = clusterManager;
        this.maxRetries = properties.getHealthCheck().getFailureThreshold();
        this.debugEnabled = properties.getDebug().isEnable();
        this.bindingByTopic = indexBindingsByTopic(properties);
    }

    private static Map<String, String> indexBindingsByTopic(KafkaClusterProperties properties) {
        Map<String, String> index = new HashMap<>();
        for (KafkaClusterProperties.ProducerConfig producer : properties.getProducers().values()) {
            String topic = producer.getTopic();
            String bindingName = KafkaClusterProperties.producerBindingName(producer.getName());
            String prev = index.put(topic, bindingName);
            if (prev != null) {
                throw new IllegalStateException(
                        "Multiple producers configured for topic '" + topic + "' — " +
                                "producers must have unique topics");
            }
        }
        return index;
    }

    /**
     * Sends a pre-built message with automatic failover across clusters.
     * Uses KafkaHeaders.KEY as idempotency key (generated UUID if absent).
     * No system headers are injected — only user-provided headers are sent.
     *
     * @param topic   destination topic
     * @param message pre-built message with payload and headers
     */
    public SendResult send(String topic, Message<?> message) {
        String id = extractOrGenerateKey(message);

        if (!message.getHeaders().containsKey(KafkaHeaders.KEY)) {
            message = MessageBuilder.fromMessage(message)
                    .setHeader(KafkaHeaders.KEY, id)
                    .build();
        }

        return doSend(topic, message, id);
    }

    /**
     * Sends a message with automatic failover across clusters.
     *
     * @param topic     destination topic
     * @param payload   any payload type — String, POJO, byte[], Map, etc.
     * @param messageId optional idempotency key, used as Kafka message key (generated if null)
     */
    public SendResult send(String topic, Object payload, String messageId) {
        return send(topic, payload, messageId, null);
    }

    /**
     * Sends a message with custom headers and automatic failover across clusters.
     *
     * @param topic     destination topic
     * @param payload   any payload type — String, POJO, byte[], Map, etc.
     * @param messageId optional idempotency key, used as Kafka message key (generated if null)
     * @param headers   optional custom headers to include in the message
     */
    public SendResult send(String topic, Object payload, String messageId, Map<String, Object> headers) {
        String id = (messageId != null) ? messageId : UUID.randomUUID().toString();

        var builder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.KEY, id);

        if (headers != null) {
            headers.forEach(builder::setHeader);
        }

        return doSend(topic, builder.build(), id);
    }

    /**
     * Sends a batch with one failover decision for the whole batch.
     *
     * <p>Sending the messages one by one would re-run the retry ladder for every message
     * against a cluster that is already gone: with 500 messages and 3 retries that is 1500
     * doomed attempts before the failover. Here the first message that reports the cluster
     * unavailable ends the attempt for the entire remainder.
     *
     * <p>On failover only the <b>unsent tail</b> moves to the next cluster. Resending the
     * whole batch would duplicate everything the previous cluster already acknowledged.
     *
     * <p>Sends stay synchronous: {@code StreamBridge.send} returns a boolean rather than a
     * future, so going async would cost the very failure signal that drives the failover.
     * Throughput belongs to {@code linger.ms} and {@code batch.size}, which already pass
     * through per-producer {@code properties.configuration}.
     *
     * @param topic    destination topic
     * @param messages pre-built messages; a missing {@code KafkaHeaders.KEY} is generated
     *                 per message, exactly as in the single-message send
     */
    public BatchSendResult sendBatch(String topic, List<Message<?>> messages) {
        requireProducerFor(topic);
        if (messages.isEmpty()) {
            return new BatchSendResult(List.of());
        }

        List<Message<?>> prepared = new ArrayList<>(messages.size());
        List<String> ids = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            String id = extractOrGenerateKey(message);
            ids.add(id);
            prepared.add(message.getHeaders().containsKey(KafkaHeaders.KEY)
                    ? message
                    : MessageBuilder.fromMessage(message).setHeader(KafkaHeaders.KEY, id).build());
        }

        List<SendResult> results = new ArrayList<>(Collections.nCopies(prepared.size(), null));

        if (!clusterManager.hasHealthyCluster()) {
            log.error("[{}] No healthy cluster available, skipping batch of {}", topic, prepared.size());
            return failRemaining(results, ids, 0);
        }

        int next = 0;
        Set<String> triedClusters = new HashSet<>();
        // Kept across clusters so the terminal line below can name the cause, not just the count.
        Exception lastError = null;

        while (next < prepared.size() && triedClusters.size() < clusterManager.getClustersByPriority().size()) {
            String cluster = clusterManager.getActiveCluster();
            if (triedClusters.contains(cluster)) {
                break;
            }

            boolean clusterLost = false;
            while (next < prepared.size() && !clusterLost) {
                String messageId = ids.get(next);
                try {
                    SendAttempt attempt = trySendWithRetries(topic, cluster, prepared.get(next), messageId);
                    if (attempt.lastError() != null) {
                        lastError = attempt.lastError();
                    }
                    if (attempt.outcome() == SendOutcome.SUCCESS) {
                        results.set(next, new SendResult(true, cluster, messageId));
                        next++;
                    } else {
                        clusterLost = true;
                    }
                } catch (SerializationException e) {
                    // A bad payload is this message's problem, not the cluster's: failing
                    // over would only reproduce it on the next cluster.
                    logSerializationError("skipping message", cluster, topic, messageId, e);
                    results.set(next, new SendResult(false, cluster, messageId));
                    next++;
                }
            }

            if (clusterLost) {
                triedClusters.add(cluster);
                if (debugEnabled && lastError != null) {
                    log.warn("[{}][{}] Cluster unavailable after {} of {} messages, failing over with the remainder",
                            cluster, topic, next, prepared.size(), lastError);
                } else {
                    log.warn("[{}][{}] Cluster unavailable after {} of {} messages, failing over with the remainder",
                            cluster, topic, next, prepared.size());
                }
                clusterManager.forceUnhealthy(cluster);
            }
        }

        if (next < prepared.size()) {
            if (debugEnabled && lastError != null) {
                log.error("[{}] All {} clusters unavailable, {} of {} messages not sent",
                        topic, triedClusters.size(), prepared.size() - next, prepared.size(), lastError);
            } else {
                log.error("[{}] All {} clusters unavailable, {} of {} messages not sent",
                        topic, triedClusters.size(), prepared.size() - next, prepared.size());
            }
        }
        BatchSendResult result = failRemaining(results, ids, next);
        log.info("[{}] Batch sent: {} of {} to {}", topic, result.sent(), result.size(), result.clusters());
        return result;
    }

    private static BatchSendResult failRemaining(List<SendResult> results, List<String> ids, int from) {
        for (int i = from; i < results.size(); i++) {
            results.set(i, new SendResult(false, null, ids.get(i)));
        }
        return new BatchSendResult(results);
    }

    private String extractOrGenerateKey(Message<?> message) {
        Object key = message.getHeaders().get(KafkaHeaders.KEY);
        if (key != null) {
            return key.toString();
        }
        return UUID.randomUUID().toString();
    }

    private void requireProducerFor(String topic) {
        if (bindingByTopic.get(topic) == null) {
            throw new IllegalArgumentException(
                    "No producer configured for topic '" + topic + "'. " +
                            "Add an entry under kafka-dr.producers");
        }
    }

    private SendResult doSend(String topic, Message<?> message, String messageId) {
        requireProducerFor(topic);

        // Fast-fail when no cluster is healthy (e.g. all clusters were down at
        // startup). Otherwise streamBridge.send() would trigger lazy binding
        // creation, and KafkaTopicProvisioner would block on metadata lookups
        // against dead brokers (max.block.ms) instead of returning a clean failure.
        if (!clusterManager.hasHealthyCluster()) {
            log.error("[{}] No healthy cluster available, skipping send: messageId={}", topic, messageId);
            return new SendResult(false, null, messageId);
        }

        Set<String> triedClusters = new HashSet<>();
        // Kept across clusters so the terminal line below can name the cause, not just the count.
        Exception lastError = null;

        while (triedClusters.size() < clusterManager.getClustersByPriority().size()) {
            String cluster = clusterManager.getActiveCluster();

            if (triedClusters.contains(cluster)) {
                break;
            }

            try {
                SendAttempt attempt = trySendWithRetries(topic, cluster, message, messageId);
                if (attempt.lastError() != null) {
                    lastError = attempt.lastError();
                }
                switch (attempt.outcome()) {
                    case SUCCESS:
                        log.info("[{}][{}] Message sent, key={}", cluster, topic, messageId);
                        return new SendResult(true, cluster, messageId);
                    case CLUSTER_UNAVAILABLE:
                        triedClusters.add(cluster);
                        // No stack trace here even in debug: trySendWithRetries already logged it.
                        log.warn("[{}][{}] Cluster unavailable, forcing failover", cluster, topic);
                        clusterManager.forceUnhealthy(cluster);
                        break;
                    case RETRIES_EXHAUSTED:
                        triedClusters.add(cluster);
                        if (debugEnabled && attempt.lastError() != null) {
                            log.warn("[{}][{}] All {} retries exhausted, forcing failover",
                                    cluster, topic, maxRetries, attempt.lastError());
                        } else {
                            log.warn("[{}][{}] All {} retries exhausted, forcing failover", cluster, topic, maxRetries);
                        }
                        clusterManager.forceUnhealthy(cluster);
                        break;
                }
            } catch (SerializationException e) {
                logSerializationError("skipping publish", cluster, topic, messageId, e);
                return new SendResult(false, cluster, messageId);
            }
        }

        if (debugEnabled && lastError != null) {
            log.error("[{}] All {} clusters unavailable, messageId={}",
                    topic, triedClusters.size(), messageId, lastError);
        } else {
            log.error("[{}] All {} clusters unavailable, messageId={}",
                    topic, triedClusters.size(), messageId);
        }
        return new SendResult(false, null, messageId);
    }

    private SendAttempt trySendWithRetries(String topic, String cluster, Message<?> originalMessage,
                                           String messageId) {
        String bindingName = bindingByTopic.get(topic);
        if (bindingName == null) {
            throw new IllegalArgumentException(
                    "No producer configured for topic '" + topic + "'. " +
                            "Add an entry under kafka-dr.producers");
        }
        Exception lastError = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (streamBridge.send(bindingName, cluster, originalMessage)) {
                    return SendAttempt.success();
                }
                // A false return carries no exception, so there is nothing to trace here.
                log.warn("[{}][{}] StreamBridge returned false", cluster, topic);
                return new SendAttempt(SendOutcome.CLUSTER_UNAVAILABLE, null);
            } catch (Exception e) {
                if (isSerializationError(e)) {
                    throw (e instanceof SerializationException se) ? se : new SerializationException(e.getMessage(), e);
                }
                if (isClusterUnavailable(e)) {
                    if (debugEnabled) {
                        log.warn("[{}][{}] Cluster unavailable: {}", cluster, topic, e.getClass().getSimpleName(), e);
                    } else {
                        log.warn("[{}][{}] Cluster unavailable: {} - {}", cluster, topic, e.getClass().getSimpleName(), e.getMessage());
                    }
                    return new SendAttempt(SendOutcome.CLUSTER_UNAVAILABLE, e);
                }
                lastError = e;
                if (debugEnabled) {
                    log.warn("[{}][{}] Attempt {}/{} failed: {}",
                            cluster, topic, attempt, maxRetries, e.getClass().getSimpleName(), e);
                } else {
                    log.warn("[{}][{}] Attempt {}/{} failed: {} - {}",
                            cluster, topic, attempt, maxRetries, e.getClass().getSimpleName(), e.getMessage());
                }
            }
        }
        return new SendAttempt(SendOutcome.RETRIES_EXHAUSTED, lastError);
    }

    private void logSerializationError(String action, String cluster, String topic,
                                       String messageId, SerializationException e) {
        if (debugEnabled) {
            log.warn("[{}][{}] Serialization error, {}: messageId={}", cluster, topic, action, messageId, e);
        } else {
            log.warn("[{}][{}] Serialization error, {}: messageId={}, error={}",
                    cluster, topic, action, messageId, e.getMessage());
        }
    }

    private enum SendOutcome { SUCCESS, CLUSTER_UNAVAILABLE, RETRIES_EXHAUSTED }

    /**
     * Outcome of the retry ladder together with the exception that ended it — null when the
     * attempt produced none (a successful send, or {@code StreamBridge.send} returning false).
     */
    private record SendAttempt(SendOutcome outcome, Exception lastError) {

        static SendAttempt success() {
            return new SendAttempt(SendOutcome.SUCCESS, null);
        }
    }

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

    /**
     * Outcome of {@link #sendBatch}, one {@link SendResult} per input message in order.
     *
     * <p>There is deliberately no single {@code cluster} field: a batch that failed over
     * mid-way was written to more than one, and the case where that matters is exactly the
     * case a single field would misreport. {@link #clusters()} names the ones actually used.
     */
    public record BatchSendResult(List<SendResult> results) {

        public BatchSendResult {
            results = List.copyOf(results);
        }

        public int size() {
            return results.size();
        }

        public int sent() {
            return (int) results.stream().filter(SendResult::success).count();
        }

        public int failed() {
            return size() - sent();
        }

        public boolean allSent() {
            return results.stream().allMatch(SendResult::success);
        }

        /** Failed messages, for retrying or reporting. */
        public List<SendResult> failures() {
            return results.stream().filter(r -> !r.success()).toList();
        }

        /** Clusters the batch actually landed on — more than one means a failover mid-batch. */
        public Set<String> clusters() {
            return results.stream()
                    .filter(SendResult::success)
                    .map(SendResult::cluster)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }
}
