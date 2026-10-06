package dev.semeshin.kafkadr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Two consumers in one application, deliberately configured differently:
 *
 * <ul>
 *   <li>{@code order-events} is consumed in batches, with per-record verdicts and
 *       manual acknowledgment;</li>
 *   <li>{@code audit-events} is consumed one record at a time, exactly as before
 *       batching existed.</li>
 * </ul>
 *
 * Batching is a per-consumer binding property, so the two coexist without any
 * special handling: each gets its own binding, listener container and function bean.
 */
@SpringBootApplication
public class MixedBatchExampleApp {
    public static void main(String[] args) {
        SpringApplication.run(MixedBatchExampleApp.class, args);
    }
}
