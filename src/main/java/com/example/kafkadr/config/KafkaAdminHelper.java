package com.example.kafkadr.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared utilities for AdminClient operations: probing clusters and provisioning topics.
 */
public final class KafkaAdminHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminHelper.class);
    private static final int DEFAULT_TIMEOUT_MS = 3000;

    private KafkaAdminHelper() {}

    public static boolean probeCluster(String brokers, int timeoutMs) {
        try (AdminClient admin = createAdminClient(brokers, timeoutMs)) {
            admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean probeCluster(String brokers) {
        return probeCluster(brokers, DEFAULT_TIMEOUT_MS);
    }

    public static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props, int timeoutMs) {
        Set<String> requiredTopics = Stream.concat(
                props.getConsumers().stream().map(KafkaClusterProperties.ConsumerConfig::getTopic),
                props.getProducers().stream().map(KafkaClusterProperties.ProducerConfig::getTopic)
        ).collect(Collectors.toSet());

        if (requiredTopics.isEmpty()) return;

        try (AdminClient admin = createAdminClient(brokers, timeoutMs)) {
            Set<String> existing = admin.listTopics().names().get(timeoutMs, TimeUnit.MILLISECONDS);
            List<NewTopic> toCreate = requiredTopics.stream()
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, Optional.empty(), Optional.empty()))
                    .toList();

            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(timeoutMs, TimeUnit.MILLISECONDS);
                log.info("Created topics on cluster '{}': {}", cluster,
                        toCreate.stream().map(NewTopic::name).toList());
            }
        } catch (Exception e) {
            log.warn("Topic provisioning failed for cluster '{}': {}", cluster, e.getMessage());
        }
    }

    public static void provisionTopics(String cluster, String brokers, KafkaClusterProperties props) {
        provisionTopics(cluster, brokers, props, DEFAULT_TIMEOUT_MS);
    }

    public static AdminClient createAdminClient(String brokers, int timeoutMs) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs,
                "socket.connection.setup.timeout.ms", timeoutMs
        ));
    }

    public static AdminClient createAdminClient(String brokers, int timeoutMs, Map<String, String> extraProps) {
        Map<String, Object> config = new HashMap<>(extraProps);
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return AdminClient.create(config);
    }
}
