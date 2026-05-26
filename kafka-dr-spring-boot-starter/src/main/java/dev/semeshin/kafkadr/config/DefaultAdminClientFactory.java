package dev.semeshin.kafkadr.config;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

public class DefaultAdminClientFactory implements AdminClientFactory {

    @Override
    public AdminClient create(String brokers, int timeoutMs, Map<String, String> kafkaClientProps) {
        return KafkaAdminHelper.createAdminClient(brokers, timeoutMs, kafkaClientProps);
    }
}
