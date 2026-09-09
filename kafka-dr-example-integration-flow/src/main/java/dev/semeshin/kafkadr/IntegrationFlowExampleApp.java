package dev.semeshin.kafkadr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;

/**
 * Spring Integration and the DR starter in one application.
 *
 * <p>Spring Cloud Stream is itself built on Spring Integration, so nothing here bridges two
 * worlds — the question is only <b>where</b> a flow is allowed to sit. This example shows the
 * two positions that keep every DR guarantee intact:
 *
 * <ul>
 *   <li><b>behind the handler</b> — the configured handler forwards the message into a
 *       {@code DirectChannel} and the flow does the work, still inside the starter's
 *       deduplicate → handle → advance-watermark sequence;</li>
 *   <li><b>in front of the producer</b> — a flow ends on {@link
 *       dev.semeshin.kafkadr.producer.ResilientProducer}, which owns cluster selection and
 *       failover, instead of a Kafka outbound adapter bound to one broker list.</li>
 * </ul>
 *
 * <p>{@code @IntegrationComponentScan} is what turns the {@code @MessagingGateway} interface
 * into a bean; Boot's Integration auto-configuration does not scan for those on its own.
 */
@SpringBootApplication
@IntegrationComponentScan
public class IntegrationFlowExampleApp {
    public static void main(String[] args) {
        SpringApplication.run(IntegrationFlowExampleApp.class, args);
    }
}
