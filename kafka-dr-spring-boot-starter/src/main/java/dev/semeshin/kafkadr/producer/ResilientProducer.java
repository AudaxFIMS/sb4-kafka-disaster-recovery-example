package dev.semeshin.kafkadr.producer;

import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ResilientProducer {

    private static final Logger log = LoggerFactory.getLogger(ResilientProducer.class);

    private final StreamBridge streamBridge;
    private final ClusterReachability reachability;
    /**
     * Clusters found answering yet lacking a topic, until when: {@code cluster|topic}. Every send to
     * such a cluster would wait {@code max.block.ms} for metadata that never comes, so for a while
     * sends fail at once instead. Short: a broker that has just restarted reports its topics
     * missing until its metadata is loaded, and a missing topic is created by hand.
     */
    private final Map<String, Instant> missingTopics = new ConcurrentHashMap<>();
    static final Duration MISSING_TOPIC_RECHECK = Duration.ofSeconds(10);
    private Clock clock = Clock.systemUTC();
    private final ActiveClusterManager clusterManager;
    /** Diagnostic logging switch — see {@code kafka-dr.debug.enable}. */
    private final boolean debugEnabled;
    private final Map<String, Route> routesByProducer = new LinkedHashMap<>();
    private final Map<String, List<Route>> routesByTopic = new HashMap<>();

    /**
     * Where one configured producer sends: its topic, the output binding, the cluster group it
     * fails over within, and the length of its retry ladder — the failure-threshold of that
     * group.
     */
    private record Route(String producer, String topic, String binding, String group, int maxRetries) {}

    /** Resolves the topology itself — for use outside a Spring context. */
    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                             KafkaClusterProperties properties) {
        this(streamBridge, clusterManager, properties, properties.topology());
    }

    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                             KafkaClusterProperties properties, ClusterTopology topology) {
        this(streamBridge, clusterManager, properties, topology, (ClusterReachability) null);
    }

    @Autowired
    public ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                             KafkaClusterProperties properties, ClusterTopology topology,
                             ObjectProvider<ClusterReachability> reachability) {
        this(streamBridge, clusterManager, properties, topology, reachability.getIfAvailable());
    }

    /**
     * @param reachability tells a cluster that lacks a topic from one that is gone; null to take
     *                     every metadata timeout as the cluster being gone
     */
    ResilientProducer(StreamBridge streamBridge, ActiveClusterManager clusterManager,
                      KafkaClusterProperties properties, ClusterTopology topology,
                      ClusterReachability reachability) {
        this.reachability = reachability;
        this.streamBridge = streamBridge;
        this.clusterManager = clusterManager;
        this.debugEnabled = properties.getDebug().isEnable();

        for (KafkaClusterProperties.ProducerConfig producer : properties.getProducers().values()) {
            ClusterTopology.Group group = topology.groupOf(producer);
            Route route = new Route(
                    producer.getName(),
                    producer.getTopic(),
                    KafkaClusterProperties.producerBindingName(producer.getName()),
                    group == null ? KafkaClusterProperties.DEFAULT_CLUSTER_GROUP : group.name(),
                    group == null ? properties.getHealthCheck().getFailureThreshold()
                            : group.healthCheck().getFailureThreshold());
            // One producer per topic and group is enforced where the topology is built.
            routesByTopic.computeIfAbsent(route.topic(), t -> new ArrayList<>()).add(route);
            routesByProducer.put(route.producer(), route);
        }
    }

    /**
     * Sends through one configured producer, addressed by its logical name — the key under
     * {@code kafka-dr.producers}. The name says nothing about the topic or the Kafka behind it,
     * so moving a topic to another cluster group, or renaming it, is a configuration change
     * only. It is also the only way to reach a topic that exists in several groups.
     *
     * @throws IllegalArgumentException when no producer has this name
     */
    public Target to(String producerName) {
        Route route = routesByProducer.get(producerName);
        if (route == null) {
            throw new IllegalArgumentException("No producer named '%s'. Configured producers: %s"
                    .formatted(producerName, routesByProducer.keySet()));
        }
        return new Target(route);
    }

    /** The sending API of one producer, as returned by {@link #to(String)}. */
    public final class Target {

        private final Route route;

        private Target(Route route) {
            this.route = route;
        }

        public String producer() { return route.producer(); }
        public String topic() { return route.topic(); }
        public String group() { return route.group(); }

        /** Same as {@link ResilientProducer#send(String, Message)}, for this producer. */
        public SendResult send(Message<?> message) {
            return sendMessage(route, message);
        }

        /** Same as {@link ResilientProducer#send(String, Object, String)}, for this producer. */
        public SendResult send(Object payload, String messageId) {
            return sendPayload(route, payload, messageId, null);
        }

        /** Same as {@link ResilientProducer#send(String, Object, String, Map)}, for this producer. */
        public SendResult send(Object payload, String messageId, Map<String, Object> headers) {
            return sendPayload(route, payload, messageId, headers);
        }

        /** Same as {@link ResilientProducer#sendBatch(String, List)}, for this producer. */
        public BatchSendResult sendBatch(List<Message<?>> messages) {
            return ResilientProducer.this.sendBatch(route, messages);
        }
    }

    /**
     * The producer of a topic, for the topic-addressed methods. Unambiguous as long as the topic
     * is configured in one cluster group; a topic that exists in several Kafkas has to be
     * addressed by producer name.
     */
    private Route routeForTopic(String topic) {
        List<Route> routes = routesByTopic.get(topic);
        if (routes == null) {
            throw new IllegalArgumentException(
                    "No producer configured for topic '" + topic + "'. " +
                            "Add an entry under kafka-dr.producers");
        }
        if (routes.size() > 1) {
            throw new IllegalArgumentException(
                    ("Topic '%s' is written by producers %s in different cluster groups. Address the one you "
                            + "mean by name: to(\"<producer>\").send(...)").formatted(topic,
                            routes.stream().map(r -> r.producer() + " (" + r.group() + ")").toList()));
        }
        return routes.get(0);
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
        return sendMessage(routeForTopic(topic), message);
    }

    private SendResult sendMessage(Route route, Message<?> message) {
        String id = extractOrGenerateKey(message);

        if (!message.getHeaders().containsKey(KafkaHeaders.KEY)) {
            message = MessageBuilder.fromMessage(message)
                    .setHeader(KafkaHeaders.KEY, id)
                    .build();
        }

        return doSend(route, message, id);
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
        return sendPayload(routeForTopic(topic), payload, messageId, headers);
    }

    private SendResult sendPayload(Route route, Object payload, String messageId, Map<String, Object> headers) {
        String id = (messageId != null) ? messageId : UUID.randomUUID().toString();

        var builder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.KEY, id);

        if (headers != null) {
            headers.forEach(builder::setHeader);
        }

        return doSend(route, builder.build(), id);
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
        return sendBatch(routeForTopic(topic), messages);
    }

    private BatchSendResult sendBatch(Route route, List<Message<?>> messages) {
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

        BatchSendResult result = deliver(route, prepared, ids);
        log.info("[{}] Batch sent: {} of {} to {}", route.topic(), result.sent(), result.size(), result.clusters());
        return result;
    }

    private SendResult doSend(Route route, Message<?> message, String messageId) {
        SendResult result = deliver(route, List.of(message), List.of(messageId)).results().get(0);
        if (result.success()) {
            log.info("[{}][{}] Message sent, key={}", result.cluster(), route.topic(), messageId);
        }
        return result;
    }

    /**
     * Delivers messages in order — a single send is a batch of one. One principle decides every
     * failed attempt: <b>a message is failed only on proof that the message is at fault, and a
     * cluster is marked down only on proof that the cluster is broken.</b> Everything in between
     * gets neither verdict — the message is reported as not taken by the group
     * ({@code ALL_CLUSTERS_FAILED}), which a {@code depends-on} bridge holds back instead of losing.
     *
     * <table>
     *   <caption>What a failed attempt on a cluster means</caption>
     *   <tr><th>Attempt ended with</th><th>Proof of</th><th>Then</th></tr>
     *   <tr><td>serialization error with no I/O cause, record too large or invalid, invalid
     *       timestamp, invalid topic name</td>
     *       <td>the message — every cluster would refuse it</td>
     *       <td>that message fails ({@code SERIALIZATION}/{@code REJECTED}); no retry, the cluster
     *       stays, the rest goes on</td></tr>
     *   <tr><td>cluster unreachable ({@code StreamBridge} false, timeout, network — a metadata
     *       timeout only when a probe confirms the cluster does not answer), or retries exhausted on
     *       a retriable Kafka error ({@code NotEnoughReplicas}…)</td>
     *       <td>the cluster</td>
     *       <td>mark it unhealthy (failover), move this message and the rest to the next cluster —
     *       also from the last one, which leaves the group unavailable</td></tr>
     *   <tr><td>anything else: a non-retriable broker answer (a missing ACL — ACLs are per
     *       cluster), a topic missing on a cluster that answers (then not waited on again for a
     *       while), the schema registry failing — see {@link #registryVerdict} — or an exception
     *       that is not Kafka's</td>
     *       <td>neither</td>
     *       <td>{@code ALL_CLUSTERS_FAILED} for it and, untried, for the rest — the cause is likely
     *       the same for all, and a bridge holds them back together. Nothing is marked, and the
     *       message is not tried elsewhere (below)</td></tr>
     * </table>
     *
     * <p>Messages go to the group's active cluster only: that is where its consumers read, and
     * replication runs from it to the standbys, not back. A message written to a standby while
     * another cluster is active would sit there unread until a failover made that standby active.
     * So a cluster that is not taking messages is either proven broken — and the whole group,
     * consumers included, fails over to the next one — or the messages are held.
     *
     * <p>A cluster lost on the way stays lost for the rest of the batch, so a dead cluster costs one
     * attempt, not one per message; once the group has no active cluster left to take them, the
     * remainder fails with {@code ALL_CLUSTERS_FAILED} without being tried.
     */
    private BatchSendResult deliver(Route route, List<Message<?>> prepared, List<String> ids) {
        String topic = route.topic();
        String group = route.group();
        List<SendResult> results = new ArrayList<>(Collections.nCopies(prepared.size(), null));

        // Fast-fail when no cluster is healthy (e.g. all clusters were down at
        // startup). Otherwise streamBridge.send() would trigger lazy binding
        // creation, and KafkaTopicProvisioner would block on metadata lookups
        // against dead brokers (max.block.ms) instead of returning a clean failure.
        if (!clusterManager.hasHealthyCluster(group)) {
            log.error("[{}] No healthy cluster available, {} message(s) not sent", topic, prepared.size());
            return failRemaining(results, ids, 0, group, Failure.NO_HEALTHY_CLUSTER);
        }

        Set<String> lost = new HashSet<>();
        // Kept across clusters so the terminal line below can name the cause, not just the count.
        Exception lastError = null;
        String cluster = null;
        int next = 0;
        boolean stranded = false;
        // The group's active cluster when last asked — losing it is what fails the group over.
        String active = null;

        // Every round sends the next message or loses a cluster, so this many rounds always suffice;
        // the bound turns a regression into a failed send instead of a thread spinning forever.
        int rounds = prepared.size() + clusterManager.getClustersByPriority(group).size() + 1;
        while (next < prepared.size() && !stranded) {
            if (rounds-- == 0) {
                log.error("[{}] Delivery made no progress; failing the remainder — this is a bug", topic);
                break;
            }
            if (cluster != null && !cluster.equals(clusterManager.getActiveCluster(group))) {
                // The group switched mid-batch — a failback, say, to a cluster that never failed.
                // The rest goes where the consumers now read, not on to the one they left.
                cluster = null;
            }
            if (cluster == null) {
                active = clusterManager.getActiveCluster(group);
                cluster = pickCluster(group, topic, lost, active);
                if (cluster == null) {
                    break;
                }
            }
            String messageId = ids.get(next);
            SendAttempt attempt;
            try {
                attempt = trySendWithRetries(route, cluster, prepared.get(next), messageId);
            } catch (MessageRefused e) {
                logRefusal("skipping message", cluster, topic, messageId, e);
                results.set(next, SendResult.failed(cluster, messageId, group, e.failure));
                next++;
                continue;
            }
            if (attempt.lastError() != null) {
                lastError = attempt.lastError();
            }

            switch (attempt.outcome()) {
                case SUCCESS -> {
                    results.set(next, SendResult.sent(cluster, messageId, group));
                    next++;
                    missingTopics.remove(cluster + "|" + topic);
                }
                case CLUSTER_UNAVAILABLE, CLUSTER_FAILED -> {
                    loseCluster(cluster, topic, attempt, next, prepared.size(), lost);
                    cluster = null;
                }
                case NOT_TAKEN -> {
                    // No proof either way: held, not sent elsewhere. The cause is likely the same for
                    // the rest — a missing ACL, a registry down — so the rest fails untried.
                    log.error("[{}][{}] Not taken, and nothing proves the message or the cluster at fault: "
                            + "holding it and the rest: messageId={}", cluster, topic, messageId);
                    results.set(next, SendResult.failed(null, messageId, group, Failure.ALL_CLUSTERS_FAILED));
                    next++;
                    stranded = true;
                }
            }
        }

        if (next < prepared.size()) {
            if (debugEnabled && lastError != null) {
                log.error("[{}] Remainder not sent — no healthy cluster of the group is left that takes it (lost, or lacking the topic): {} of {} messages",
                        topic, prepared.size() - next, prepared.size(), lastError);
            } else {
                log.error("[{}] Remainder not sent — no healthy cluster of the group is left that takes it (lost, or lacking the topic): {} of {} messages",
                        topic, prepared.size() - next, prepared.size());
            }
        }
        return failRemaining(results, ids, next, group, Failure.ALL_CLUSTERS_FAILED);
    }

    /** Marks the cluster in use unhealthy on proof that it is broken, which fails the group over. */
    private void loseCluster(String cluster, String topic, SendAttempt attempt, int sent, int total,
                             Set<String> lost) {
        lost.add(cluster);
        String progress = total > 1 ? " after %d of %d messages".formatted(sent, total) : "";
        if (debugEnabled && attempt.lastError() != null) {
            log.warn("[{}][{}] Cluster failed ({}){}, failing over", cluster, topic, attempt.outcome(), progress,
                    attempt.lastError());
        } else {
            log.warn("[{}][{}] Cluster failed ({}){}, failing over", cluster, topic, attempt.outcome(), progress);
        }
        clusterManager.forceUnhealthy(cluster);
    }

    /**
     * The group's active cluster, or null when it cannot take the messages: lost in this send, not
     * healthy — the manager keeps an unhealthy cluster active when no other is left — or known to
     * lack the topic. Never another cluster: only the active one is read.
     */
    private String pickCluster(String group, String topic, Set<String> lost, String active) {
        // Health first: a recheck of the topic is spent only on a cluster that would be tried.
        if (active != null && !lost.contains(active)
                && Boolean.TRUE.equals(clusterManager.getHealthStatuses(group).get(active))
                && !isTopicMissing(active, topic)) {
            return active;
        }
        return null;
    }

    private static BatchSendResult failRemaining(List<SendResult> results, List<String> ids, int from,
                                                 String group, Failure failure) {
        for (int i = from; i < results.size(); i++) {
            results.set(i, SendResult.failed(null, ids.get(i), group, failure));
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

    private SendAttempt trySendWithRetries(Route route, String cluster, Message<?> originalMessage,
                                           String messageId) {
        String topic = route.topic();
        String bindingName = route.binding();
        Exception lastError = null;
        boolean registryOutage = false;
        int maxRetries = route.maxRetries();
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (streamBridge.send(bindingName, cluster, originalMessage)) {
                    return SendAttempt.success();
                }
                // A false return carries no exception, so there is nothing to trace here.
                log.warn("[{}][{}] StreamBridge returned false", cluster, topic);
                return new SendAttempt(SendOutcome.CLUSTER_UNAVAILABLE, null);
            } catch (Exception e) {
                Failure refusal = refusalOf(e);
                if (refusal != null) {
                    throw new MessageRefused(refusal, e);
                }
                Registry registry = registryVerdict(e);
                if (registry == Registry.NOT_HERE) {
                    log.warn("[{}][{}] Not taken — this cluster's schema registry: {} - {}",
                            cluster, topic, e.getClass().getSimpleName(), e.getMessage());
                    return new SendAttempt(SendOutcome.NOT_TAKEN, e);
                }
                if (registry == Registry.OUTAGE) {
                    // The serializer could not reach its schema registry, or the registry failed.
                    // Confluent reports some of that as Kafka's own TimeoutException — not the
                    // broker's, and no proof of anything: retried, then not taken.
                    lastError = e;
                    registryOutage = true;
                    log.warn("[{}][{}] Attempt {}/{}: schema registry failed: {} - {}",
                            cluster, topic, attempt, maxRetries, e.getClass().getSimpleName(), e.getMessage());
                    continue;
                }
                registryOutage = false;
                if (isClusterUnavailable(e) && isMissingFromMetadata(e)
                        && reachability != null && reachability.isReachable(cluster)) {
                    // Waited for metadata, yet the cluster answers: the topic is missing there.
                    // Not proof of a broken cluster — and asking again gets the same answer.
                    log.warn("[{}][{}] Cluster answers, but the topic is not in its metadata: {}",
                            cluster, topic, rootMessage(e));
                    markTopicMissing(cluster, topic);
                    return new SendAttempt(SendOutcome.NOT_TAKEN, e);
                }
                if (isClusterUnavailable(e) && hasKafkaTimeout(e) && !isMissingFromMetadata(e)
                        && reachability != null && reachability.lacksTopic(cluster, topic)) {
                    // Timed out, yet the cluster answers that it has no such topic: deleted while
                    // this producer still knew it. Not proof of a broken cluster.
                    log.warn("[{}][{}] Cluster answers that the topic does not exist: {}", cluster, topic, rootMessage(e));
                    markTopicMissing(cluster, topic);
                    return new SendAttempt(SendOutcome.NOT_TAKEN, e);
                }
                if (isClusterUnavailable(e)) {
                    if (debugEnabled) {
                        log.warn("[{}][{}] Cluster unavailable: {}", cluster, topic, e.getClass().getSimpleName(), e);
                    } else {
                        log.warn("[{}][{}] Cluster unavailable: {} - {}", cluster, topic, e.getClass().getSimpleName(), e.getMessage());
                    }
                    return new SendAttempt(SendOutcome.CLUSTER_UNAVAILABLE, e);
                }
                if (isDefinitiveAnswer(e)) {
                    // The broker's answer to this request; asking again gets the same answer.
                    log.warn("[{}][{}] Not taken: {} - {}", cluster, topic, e.getClass().getSimpleName(), e.getMessage());
                    return new SendAttempt(SendOutcome.NOT_TAKEN, e);
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
        // A retriable Kafka error that outlasts the retries is the cluster's; anything else proves
        // nothing about the cluster.
        return new SendAttempt(!registryOutage && isRetriable(lastError) ? SendOutcome.CLUSTER_FAILED
                : SendOutcome.NOT_TAKEN, lastError);
    }

    private void markTopicMissing(String cluster, String topic) {
        missingTopics.put(cluster + "|" + topic, clock.instant().plus(MISSING_TOPIC_RECHECK));
    }

    private boolean isTopicMissing(String cluster, String topic) {
        Instant until = missingTopics.get(cluster + "|" + topic);
        if (until == null) {
            return false;
        }
        Instant now = clock.instant();
        if (now.isBefore(until)) {
            return true;
        }
        // One sender rechecks — and may wait max.block.ms doing so; the others keep failing at
        // once until it has. A send that succeeds there clears the entry.
        return !missingTopics.replace(cluster + "|" + topic, until, now.plus(MISSING_TOPIC_RECHECK));
    }

    /** For tests that move time. */
    void setClock(Clock clock) {
        this.clock = clock;
    }

    /** The innermost message: wrappers such as Spring's MessageHandlingException often carry none. */
    private static String rootMessage(Throwable e) {
        String message = e.getMessage();
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null) {
                message = t.getClass().getSimpleName() + ": " + t.getMessage();
            }
        }
        return message;
    }

    private static boolean hasKafkaTimeout(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TimeoutException) {
                return true;
            }
        }
        return false;
    }

    /** Kafka's "topic not present in metadata" — a missing topic and a dead cluster alike. */
    private static boolean isMissingFromMetadata(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TimeoutException && t.getMessage() != null
                    && t.getMessage().contains("not present in metadata")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isRetriable(Throwable e) {
        while (e != null) {
            if (e instanceof RetriableException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    /**
     * A non-retriable answer of the broker to this request — a missing ACL, a policy violation.
     * Proof of nothing: neither of the message (such settings are per cluster, and are not
     * replicated) nor of a broken cluster. Not retried — asking again gets the same answer.
     */
    private static boolean isDefinitiveAnswer(Throwable e) {
        while (e != null) {
            if (e instanceof RetriableException) {
                return false;
            }
            if (e instanceof ApiException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private void logRefusal(String action, String cluster, String topic, String messageId, MessageRefused e) {
        String what = e.failure == Failure.SERIALIZATION ? "Serialization error" : "Message rejected by the broker";
        if (debugEnabled) {
            log.warn("[{}][{}] {}, {}: messageId={}", cluster, topic, what, action, messageId, e.getCause());
        } else {
            log.warn("[{}][{}] {}, {}: messageId={}, error={}",
                    cluster, topic, what, action, messageId, e.getCause().getMessage());
        }
    }

    /**
     * The message itself cannot be sent — no cluster would take it. Thrown out of the retry ladder
     * so the caller fails that message without failing over, which would only take the next
     * cluster down with the same message.
     */
    private static final class MessageRefused extends RuntimeException {
        private final Failure failure;

        MessageRefused(Failure failure, Throwable cause) {
            super(cause.getMessage(), cause, false, false);
            this.failure = failure;
        }
    }

    private enum SendOutcome {
        SUCCESS,
        /** The cluster could not be reached. */
        CLUSTER_UNAVAILABLE,
        /** Reachable, but the retries ran out on a retriable Kafka error: the cluster is broken. */
        CLUSTER_FAILED,
        /** Not taken, for a reason that proves nothing about the message or the cluster. */
        NOT_TAKEN
    }

    /**
     * Outcome of the retry ladder together with the exception that ended it — null when the
     * attempt produced none (a successful send, or {@code StreamBridge.send} returning false).
     */
    private record SendAttempt(SendOutcome outcome, Exception lastError) {

        static SendAttempt success() {
            return new SendAttempt(SendOutcome.SUCCESS, null);
        }
    }

    /**
     * Why the message itself cannot be sent — only on proof that every cluster would answer the
     * same — or null. These are properties of the record: its bytes, its size, its timestamp, its
     * topic name, which is the same on every cluster.
     *
     * <p>Not here on purpose: a missing ACL (per cluster, not replicated), a
     * {@code CorruptRecordException} (retriable, a CRC error in transit), and a serializer that
     * failed on I/O — a schema registry that cannot be reached is the registry's problem, and
     * every cluster may have its own.
     */
    private static Failure refusalOf(Throwable e) {
        while (e != null) {
            // StreamBridge converts a payload without a native serializer itself, and reports a
            // payload it cannot convert this way rather than as a Kafka SerializationException.
            if (e instanceof SerializationException || e instanceof MessageConversionException) {
                Registry registry = registryVerdict(e);
                return registry == Registry.OUTAGE || registry == Registry.NOT_HERE ? null : Failure.SERIALIZATION;
            }
            if (e instanceof RecordTooLargeException
                    || e instanceof RecordBatchTooLargeException
                    || e instanceof InvalidRecordException
                    || e instanceof InvalidTimestampException
                    || e instanceof InvalidTopicException) {
                return Failure.REJECTED;
            }
            e = e.getCause();
        }
        return null;
    }

    private enum Registry {
        /** Not the registry. */
        NONE,
        /** The registry judged the message itself: an invalid schema. */
        MESSAGE,
        /** This cluster's registry answered for its own state — the subject is not there, the
         *  schema conflicts with its own history. Asking again gets the same answer; another
         *  cluster's registry may answer otherwise. */
        NOT_HERE,
        /** The registry could not be reached, or failed. */
        OUTAGE
    }

    /**
     * What a failure says about the schema registry behind the serializer, as Confluent's
     * serializers (8.x) report it — matched by name and origin, so the starter needs no registry
     * client on its classpath. Every cluster may have its own registry, with its own subjects,
     * history and mode, so only what is wrong with the schema itself is the message's:
     * <ul>
     *   <li>a {@code RestClientException} anywhere in the chain carries the registry's HTTP status
     *       and error code. 404 (subject not found), 409 (incompatible with this registry's history)
     *       and error code 42205 (operation not permitted — a registry in {@code READONLY} or
     *       {@code IMPORT} mode, as a standby's replica is) are this registry's state. 401, 403
     *       (its credentials), 408, 429, 5xx and error code 50005 (a reply that was not the
     *       registry's — a proxy, a wrong path) are an outage. Other 4xx — 422, an invalid schema —
     *       are the message's. Confluent wraps 408/5xx in Kafka's own {@code TimeoutException} and
     *       502 in a {@code DisconnectException}, which would otherwise read as the broker gone;</li>
     *   <li>an {@code IOException} under a serializer's {@code SerializationException}, or under
     *       the {@code TimeoutException} Confluent turns a read timeout into, is told apart by where
     *       it came from: a network failure, or anything thrown inside the registry client — a
     *       reset connection, a proxy's page that is not JSON — is an outage; Confluent's
     *       "Incompatible schema" against this registry's latest version is this registry's state;
     *       anything else, such as Jackson failing on the payload, is the message's;</li>
     *   <li>a {@code ThrottlingQuotaExceededException} is the registry's 429, which Confluent
     *       throws without a cause; brokers throttle produce requests by delaying them, not so.</li>
     * </ul>
     */
    private static Registry registryVerdict(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().equals("RestClientException")) {
                int status = intProperty(t, "getStatus");
                int errorCode = intProperty(t, "getErrorCode");
                if (errorCode == 50005 || status < 400 || status >= 500
                        || status == 401 || status == 403 || status == 408 || status == 429) {
                    return Registry.OUTAGE;
                }
                return status == 404 || status == 409 || errorCode == 42205 ? Registry.NOT_HERE : Registry.MESSAGE;
            }
            if (t instanceof ThrottlingQuotaExceededException) {
                return Registry.OUTAGE;
            }
            if (t instanceof SerializationException || t instanceof TimeoutException
                    || t instanceof DisconnectException) {
                java.io.IOException io = firstIoException(t.getCause());
                if (io != null) {
                    if (isNetworkIo(io) || thrownByRegistryClient(io)) {
                        return Registry.OUTAGE;
                    }
                    if (io.getMessage() != null && io.getMessage().startsWith("Incompatible schema")) {
                        return Registry.NOT_HERE;
                    }
                    return Registry.NONE;
                }
            }
        }
        return Registry.NONE;
    }

    private static java.io.IOException firstIoException(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof java.io.IOException io) {
                return io;
            }
            if (t.getClass().getSimpleName().equals("RestClientException")) {
                return null;
            }
        }
        return null;
    }

    private static boolean thrownByRegistryClient(Throwable t) {
        for (StackTraceElement frame : t.getStackTrace()) {
            if (frame.getClassName().startsWith("io.confluent.kafka.schemaregistry.client.")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNetworkIo(Throwable t) {
        return t instanceof java.net.SocketException
                || t instanceof java.net.UnknownHostException
                || t instanceof java.io.InterruptedIOException
                || t instanceof javax.net.ssl.SSLException;
    }

    private static int intProperty(Throwable t, String getter) {
        try {
            Object value = t.getClass().getMethod(getter).invoke(t);
            return value instanceof Integer code ? code : -1;
        } catch (ReflectiveOperationException e) {
            return -1;
        }
    }

    private boolean isClusterUnavailable(Throwable e) {
        while (e != null) {
            if (e instanceof SerializationException) {
                // Below it is the serializer's own I/O — a schema registry — not the broker's.
                return false;
            }
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

    /** Why a send did not reach Kafka. */
    public enum Failure {
        /** No cluster of the group was healthy, so nothing was attempted. */
        NO_HEALTHY_CLUSTER,
        /**
         * No cluster of the group took the message, and nothing proves the message at fault: the
         * clusters were unreachable or broken, or did not take it for a reason that is no verdict on
         * the message — a missing ACL, a schema registry down. A later retry may succeed; a
         * {@code depends-on} bridge holds the record back, for at most {@code depends-on-max-hold-ms}
         * while the group itself is up.
         */
        ALL_CLUSTERS_FAILED,
        /** The message itself could not be serialized; another cluster would fail the same way. */
        SERIALIZATION,
        /**
         * The broker refused the record — too large, invalid, an invalid timestamp, or the topic
         * name is invalid. Not a cluster failure: no failover is triggered, and another cluster
         * would refuse it the same way.
         */
        REJECTED;

        /** True when the group, not the message, is the problem — a later retry may succeed. */
        public boolean groupUnavailable() {
            return this == NO_HEALTHY_CLUSTER || this == ALL_CLUSTERS_FAILED;
        }
    }

    /**
     * Outcome of one send.
     *
     * @param cluster binder id of the cluster that took the message, or the one that rejected it
     * @param group   cluster group the producer belongs to
     * @param failure why the send failed; null on success
     */
    public record SendResult(boolean success, String cluster, String messageId, String group, Failure failure) {

        /** The pre-group shape, kept for callers that build results themselves. */
        public SendResult(boolean success, String cluster, String messageId) {
            this(success, cluster, messageId, null, success ? null : Failure.ALL_CLUSTERS_FAILED);
        }

        static SendResult sent(String cluster, String messageId, String group) {
            return new SendResult(true, cluster, messageId, group, null);
        }

        static SendResult failed(String cluster, String messageId, String group, Failure failure) {
            return new SendResult(false, cluster, messageId, group, failure);
        }

        /**
         * This result if the message was sent; otherwise an exception, so a caller can let the
         * failure propagate — from a consumer handler, that is what gets the source record
         * redelivered instead of committed.
         *
         * @throws ClusterGroupUnavailableException when no cluster of the group took the message
         * @throws SendFailedException              when the message itself could not be sent
         */
        public SendResult orThrow() {
            if (success) {
                return this;
            }
            Failure reason = failure == null ? Failure.ALL_CLUSTERS_FAILED : failure;
            String message = "Message %s was not sent to cluster group '%s': %s".formatted(messageId, group, reason);
            if (reason.groupUnavailable()) {
                throw new ClusterGroupUnavailableException(message, group, messageId, reason);
            }
            throw new SendFailedException(message, group, messageId, reason);
        }
    }

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

        /**
         * This result if every message was sent; otherwise an exception naming how many were
         * not. A failure of the group outranks a failure of single messages: it is the one a
         * later retry can fix.
         *
         * @throws ClusterGroupUnavailableException when messages failed because no cluster took them
         * @throws SendFailedException              when only single messages failed, each proven at
         *                                          fault — could not be serialized, or rejected by the
         *                                          broker
         */
        public BatchSendResult orThrow() {
            if (allSent()) {
                return this;
            }
            List<SendResult> failures = failures();
            SendResult first = failures.stream()
                    .filter(r -> r.failure() == null || r.failure().groupUnavailable())
                    .findFirst()
                    .orElse(failures.get(0));
            Failure reason = first.failure() == null ? Failure.ALL_CLUSTERS_FAILED : first.failure();
            String message = "%d of %d messages were not sent to cluster group '%s': %s"
                    .formatted(failed(), size(), first.group(), reason);
            if (reason.groupUnavailable()) {
                throw new ClusterGroupUnavailableException(message, first.group(), null, reason);
            }
            throw new SendFailedException(message, first.group(), null, reason);
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
