package dev.semeshin.kafkadr.config;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

/**
 * Factory for Kafka AdminClient. Extracted as an interface so tests can
 * inject a mock without spinning up a real Kafka broker.
 */
public interface AdminClientFactory {

    AdminClient create(String brokers, int timeoutMs, Map<String, String> kafkaClientProps);
}
