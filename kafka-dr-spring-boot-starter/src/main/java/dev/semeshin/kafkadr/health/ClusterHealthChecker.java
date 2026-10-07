package dev.semeshin.kafkadr.health;

import dev.semeshin.kafkadr.concurrent.DaemonExecutors;
import dev.semeshin.kafkadr.config.AdminClientFactory;
import dev.semeshin.kafkadr.config.KafkaAdminHelper;
import dev.semeshin.kafkadr.config.KafkaClusterProperties;
import dev.semeshin.kafkadr.config.ClusterTopology;
import dev.semeshin.kafkadr.config.KafkaClusterProperties.HealthCheckConfig;
import dev.semeshin.kafkadr.producer.ClusterReachability;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Probes every cluster and reports the result to {@link ActiveClusterManager}.
 *
 * <p>Each cluster group runs on its own schedule, at its own {@code health-check.interval-ms},
 * with its own scheduler thread and its own probe pool: a group with a tighter failover budget
 * can be probed more often than the rest, and probes of one group that hang — an AdminClient
 * waiting out its close against a black-holed broker — can only ever queue behind each other,
 * never delay another group's probes past their deadline and fail it over for nothing.
 *
 * <p>The schedules are started with the application context and cancelled before it shuts down.
 * A round interrupted by the shutdown reports nothing: an interrupted wait says nothing about the
 * cluster, and reporting it as a failure would fail the group over — and persist that failover —
 * on the way down.
 */
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@Component
public class ClusterHealthChecker implements HealthIndicator, SmartLifecycle, ClusterReachability {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealthChecker.class);

    private final KafkaClusterProperties properties;
    private final ClusterTopology topology;
    private final ActiveClusterManager clusterManager;
    private final AdminClientFactory adminClientFactory;
    /** Probe pool per group, one thread per cluster of that group. */
    private final Map<String, ExecutorService> probeExecutors = new LinkedHashMap<>();
    /** Kafka client properties per cluster id, resolved once — they do not change at run time. */
    private final Map<String, Map<String, String>> clientProperties = new LinkedHashMap<>();
    private final List<ScheduledFuture<?>> schedules = new ArrayList<>();
    private ThreadPoolTaskScheduler scheduler;
    private volatile boolean running;
    /** Set once stopping begins; a round still in flight then reports nothing. */
    private volatile boolean stopping;

    /** Resolves the topology itself — for use outside a Spring context. */
    public ClusterHealthChecker(KafkaClusterProperties properties,
                                ActiveClusterManager clusterManager,
                                AdminClientFactory adminClientFactory) {
        this(properties, properties.topology(), clusterManager, adminClientFactory);
    }

    @Autowired
    public ClusterHealthChecker(KafkaClusterProperties properties,
                                ClusterTopology topology,
                                ActiveClusterManager clusterManager,
                                AdminClientFactory adminClientFactory) {
        this.properties = properties;
        this.topology = topology;
        this.clusterManager = clusterManager;
        this.adminClientFactory = adminClientFactory;
        for (ClusterTopology.Group group : topology.groups()) {
            // One thread per cluster so a slow/unreachable cluster never delays the others.
            String prefix = "kafka-dr-health-probe-" + group.name() + "-";
            probeExecutors.put(group.name(), DaemonExecutors.fixedPool(prefix, group.clusters().size()));
            for (ClusterTopology.ClusterRef ref : group.clusters()) {
                clientProperties.put(ref.id(), Map.copyOf(KafkaAdminHelper.extractKafkaClientProperties(
                        properties.getEffectiveEnvironment(ref.id()))));
            }
        }
    }

    /**
     * Schedules every group at fixed rate (not fixed delay) so the cadence is wall-clock —
     * the interval is not stretched by how long the probes take. Probes run in parallel and
     * each is bounded by timeout-ms, so a round can't exceed roughly that bound.
     */
    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        stopping = false;
        // One scheduler thread per group, so one group's round never waits for another's.
        scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(Math.max(1, topology.groups().size()));
        scheduler.setThreadNamePrefix("kafka-dr-health-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        for (ClusterTopology.Group group : topology.groups()) {
            Duration interval = Duration.ofMillis(group.healthCheck().getIntervalMs());
            schedules.add(scheduler.scheduleAtFixedRate(() -> checkGroupSafely(group), interval));
        }
        running = true;
    }

    @Override
    public synchronized void stop() {
        // First, so a round interrupted by the shutdown below knows not to report.
        stopping = true;
        schedules.forEach(schedule -> schedule.cancel(false));
        schedules.clear();
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @PreDestroy
    public void shutdown() {
        stop();
        probeExecutors.values().forEach(ExecutorService::shutdownNow);
    }

    /** Probes every cluster of every group once, group after group. */
    public void checkAllClusters() {
        topology.groups().forEach(this::checkGroup);
    }

    /** Probes the clusters of one group in parallel and reports each result. */
    public void checkGroup(String group) {
        checkGroup(topology.group(group));
    }

    /** A failing round must not cancel the schedule: the next one may well succeed. */
    private void checkGroupSafely(ClusterTopology.Group group) {
        try {
            checkGroup(group);
        } catch (RuntimeException e) {
            if (running) {
                log.error("[{}] Health check round failed", group.name(), e);
            }
        }
    }

    private void checkGroup(ClusterTopology.Group group) {
        HealthCheckConfig healthCheck = group.healthCheck();
        long timeout = healthCheck.getTimeoutMs();
        boolean deepProbe = healthCheck.isDeepProbe();
        Set<String> topics = group.topics();
        // Hard ceiling on how long we wait for a probe result. The deep probe issues up
        // to three sequential admin calls, each bounded by timeout; basic probe just one.
        long awaitMs = (deepProbe ? timeout * 3 : timeout) + 2000;

        ExecutorService probes = probeExecutors.get(group.name());
        Map<String, Future<Boolean>> inFlight = new LinkedHashMap<>();
        // Reported in priority order, not declaration order: the initial election takes the first
        // healthy report, which then is the best healthy cluster whatever the YAML order.
        for (ClusterTopology.ClusterRef ref : group.clustersByPriority()) {
            String name = ref.id();
            String brokers = ref.bootstrapServers();
            Map<String, String> kafkaClientProps = clientProperties.get(name);

            inFlight.put(name, probes.submit(() -> deepProbe
                    ? probeDeep(name, brokers, timeout, kafkaClientProps, healthCheck, topics)
                    : probeAdmin(brokers, timeout, kafkaClientProps)));
        }

        for (Map.Entry<String, Future<Boolean>> entry : inFlight.entrySet()) {
            String name = entry.getKey();
            boolean healthy;
            try {
                healthy = entry.getValue().get(awaitMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // The round was cut short — by the shutdown, typically. That is no verdict on
                // any cluster: report nothing and let the next round, if any, decide.
                inFlight.values().forEach(future -> future.cancel(true));
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                entry.getValue().cancel(true);
                if (debugEnabled()) {
                    log.warn("[{}] Health probe did not complete within {}ms", name, awaitMs, e);
                } else {
                    log.debug("[{}] Health probe did not complete within {}ms: {}", name, awaitMs, e.getMessage());
                }
                healthy = false;
            }
            if (stopping) {
                return;
            }
            clusterManager.reportHealth(name, healthy);
        }
    }

    /**
     * One basic probe of one cluster, on the caller's thread, bounded by its group's
     * {@code timeout-ms}. Reports nothing to the cluster manager.
     */
    @Override
    public boolean isReachable(String clusterId) {
        for (ClusterTopology.Group group : topology.groups()) {
            for (ClusterTopology.ClusterRef ref : group.clusters()) {
                if (ref.id().equals(clusterId)) {
                    return probeAdmin(ref.bootstrapServers(), group.healthCheck().getTimeoutMs(),
                            clientProperties.get(ref.id()));
                }
            }
        }
        return false;
    }

    /** One topic lookup on one cluster, on the caller's thread, bounded by its group's timeout-ms. */
    @Override
    public boolean lacksTopic(String clusterId, String topic) {
        for (ClusterTopology.Group group : topology.groups()) {
            for (ClusterTopology.ClusterRef ref : group.clusters()) {
                if (ref.id().equals(clusterId)) {
                    long timeoutMs = group.healthCheck().getTimeoutMs();
                    try (AdminClient admin = adminClientFactory.create(ref.bootstrapServers(), (int) timeoutMs,
                            clientProperties.get(ref.id()))) {
                        admin.describeTopics(List.of(topic)).allTopicNames().get(timeoutMs, TimeUnit.MILLISECONDS);
                        return false;
                    } catch (ExecutionException e) {
                        return e.getCause() instanceof UnknownTopicOrPartitionException;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Basic probe: checks cluster metadata via AdminClient.describeCluster().
     * Catches: controller/broker process down, network unreachable.
     * Misses: broker alive but not accepting data (no partition leaders).
     */
    private boolean probeAdmin(String brokers, long timeoutMs, Map<String, String> kafkaClientProps) {
        try (AdminClient admin = adminClientFactory.create(brokers, (int) timeoutMs, kafkaClientProps)) {
            admin.describeCluster()
                    .clusterId()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            if (debugEnabled()) {
                log.warn("[{}] Admin probe failed", brokers, e);
            } else {
                log.debug("[{}] Admin probe failed: {}", brokers, e.getMessage());
            }
            return false;
        }
    }

    /**
     * Deep probe: describeCluster + describeTopics to verify partition leaders exist.
     * Catches everything basic probe catches, plus: broker alive but partitions have
     * no leaders (all replicas offline, broker in maintenance, etc.).
     * No data is written — read-only metadata check.
     */
    private boolean probeDeep(String clusterName, String brokers, long timeoutMs,
                              Map<String, String> kafkaClientProps, HealthCheckConfig healthCheck,
                              Set<String> groupTopics) {
        try (AdminClient admin = adminClientFactory.create(brokers, (int) timeoutMs, kafkaClientProps)) {
            // Step 1: cluster must be reachable
            admin.describeCluster()
                    .clusterId()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            // Step 2: check that the group's topics have partition leaders. Topics of other
            // groups live in another Kafka and are not expected here.
            Set<String> topics = new HashSet<>(groupTopics);
            if (topics.isEmpty()) {
                return true;
            }

            // Only check topics that actually exist on this cluster
            Set<String> existingTopics = admin.listTopics()
                    .names()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            topics.retainAll(existingTopics);

            if (topics.isEmpty()) {
                return true;
            }

            Map<String, TopicDescription> descriptions = admin.describeTopics(topics)
                    .allTopicNames()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            int minNodes = healthCheck.getDeepProbeMinNodes();
            int minIsr = healthCheck.getDeepProbeMinIsr();

            for (Map.Entry<String, TopicDescription> desc : descriptions.entrySet()) {
                String topicName = desc.getKey();
                Set<Integer> topicActiveNodes = new HashSet<>();

                for (TopicPartitionInfo partition : desc.getValue().partitions()) {
                    Node leader = partition.leader();
                    if (leader != null && leader.id() != Node.noNode().id()) {
                        topicActiveNodes.add(leader.id());
                    }

                    if (minIsr > 0 && partition.isr().size() < minIsr) {
                        log.warn("DR_EVENT [{}][{}] Partition {} ISR={}, required {}",
                                clusterName, topicName, partition.partition(),
                                partition.isr().size(), minIsr);
                        return false;
                    }
                }

                if (topicActiveNodes.size() < minNodes) {
                    log.warn("DR_EVENT [{}][{}] Only {} active node(s), required {}",
                            clusterName, topicName, topicActiveNodes.size(), minNodes);
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            if (debugEnabled()) {
                log.warn("[{}] Deep probe failed", clusterName, e);
            } else {
                log.debug("[{}] Deep probe failed: {}", clusterName, e.getMessage());
            }
            return false;
        }
    }

    /** Diagnostic logging switch — see {@code kafka-dr.debug.enable}. */
    private boolean debugEnabled() {
        return properties.getDebug().isEnable();
    }

    /**
     * One group keeps the flat layout it has always had. With several, every detail is
     * prefixed with its group, since each group has its own active cluster.
     */
    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        if (!topology.isMultiGroup()) {
            builder.withDetail("activeCluster", clusterManager.getActiveCluster());
            clusterManager.getHealthStatuses()
                    .forEach((name, healthy) -> builder.withDetail("cluster." + name, healthy ? "UP" : "DOWN"));
            return builder.build();
        }
        for (ClusterTopology.Group group : topology.groups()) {
            String prefix = "group." + group.name() + ".";
            builder.withDetail(prefix + "activeCluster", clusterManager.getActiveCluster(group.name()));
            Map<String, Boolean> statuses = clusterManager.getHealthStatuses(group.name());
            for (ClusterTopology.ClusterRef ref : group.clusters()) {
                builder.withDetail(prefix + "cluster." + ref.name(),
                        Boolean.TRUE.equals(statuses.get(ref.id())) ? "UP" : "DOWN");
            }
        }
        return builder.build();
    }
}
