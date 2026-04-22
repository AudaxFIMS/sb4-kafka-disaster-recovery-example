package dev.semeshin.kafkadr;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Auto-configuration for Kafka DR framework.
 * Activated when kafka-dr.enabled=true. Scans the framework's component packages
 * so the starter works regardless of the consuming application's base package.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "kafka-dr.enabled", havingValue = "true")
@ComponentScan("dev.semeshin.kafkadr")
@EnableScheduling
public class KafkaDrAutoConfiguration {
}
