package dev.semeshin.kafkadr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Two independent Kafkas in one application, each a cluster group with its own primary and
 * standby, its own failover and its own MirrorMaker:
 *
 * <ul>
 *   <li>{@code core} carries the orders and receives the scores back;</li>
 *   <li>{@code analytics} receives the orders and computes the scores.</li>
 * </ul>
 *
 * Orders cross into analytics and scores cross back, each bridge declaring the group it writes
 * to under {@code depends-on}: while that group is down the bridge is paused and nothing is
 * skipped. Topic {@code audit} exists in both groups under the same consumer group — two
 * different topics in two different Kafkas, addressed by producer name.
 */
@SpringBootApplication
public class MultiGroupExampleApp {
    public static void main(String[] args) {
        SpringApplication.run(MultiGroupExampleApp.class, args);
    }
}
