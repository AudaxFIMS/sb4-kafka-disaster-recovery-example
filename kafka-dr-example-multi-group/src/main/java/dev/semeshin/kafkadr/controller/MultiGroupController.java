package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.handler.AuditProcessor;
import dev.semeshin.kafkadr.handler.OrderBridgeProcessor;
import dev.semeshin.kafkadr.handler.ScoreSinkProcessor;
import dev.semeshin.kafkadr.handler.ScoringProcessor;
import dev.semeshin.kafkadr.model.OrderEvent;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class MultiGroupController {

    private static final int MAX_ORDERS = 1000;

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;
    private final OrderBridgeProcessor bridge;
    private final ScoringProcessor scoring;
    private final ScoreSinkProcessor sink;
    private final AuditProcessor audit;

    public MultiGroupController(ResilientProducer producer,
                                ActiveClusterManager clusterManager,
                                OrderBridgeProcessor bridge,
                                ScoringProcessor scoring,
                                ScoreSinkProcessor sink,
                                AuditProcessor audit) {
        this.producer = producer;
        this.clusterManager = clusterManager;
        this.bridge = bridge;
        this.scoring = scoring;
        this.sink = sink;
        this.audit = audit;
    }

    /**
     * Publishes orders to {@code core}. Each makes the round trip core -> analytics -> core,
     * visible in the counters of {@link #status()}.
     */
    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> sendOrders(
            @RequestParam(defaultValue = "5") int count,
            @RequestParam(defaultValue = "100") int amount,
            @RequestParam(defaultValue = "acme") String customer) {

        if (count < 1 || count > MAX_ORDERS) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "count must be between 1 and " + MAX_ORDERS));
        }
        List<Message<?>> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            OrderEvent order = new OrderEvent(UUID.randomUUID().toString(), amount, customer);
            messages.add(MessageBuilder.withPayload(order).setHeader(KafkaHeaders.KEY, order.orderId()).build());
        }
        ResilientProducer.BatchSendResult result = producer.to("orders").sendBatch(messages);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("requested", count);
        response.put("sent", result.sent());
        response.put("failed", result.failed());
        response.put("clusters", result.clusters());
        return ResponseEntity.ok(response);
    }

    /**
     * Writes topic {@code audit} in the chosen group. Both groups have a topic of that name, so
     * only the producer name says which one: {@code core-audit} or {@code analytics-audit}.
     */
    @PostMapping("/audit")
    public ResponseEntity<Map<String, Object>> sendAudit(
            @RequestParam(defaultValue = "core") String group,
            @RequestParam String message) {

        if (!clusterManager.getGroups().contains(group)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "unknown cluster group '" + group + "'",
                    "groups", clusterManager.getGroups()));
        }
        ResilientProducer.SendResult result = producer.to(group + "-audit")
                .send(message, UUID.randomUUID().toString());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.success());
        response.put("group", result.group());
        response.put("cluster", result.cluster());
        response.put("failure", result.failure());
        return ResponseEntity.ok(response);
    }

    /** Per-group state — each group has its own active cluster — and the round-trip counters. */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> groups = new LinkedHashMap<>();
        for (String group : clusterManager.getGroups()) {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("activeCluster", clusterManager.getActiveCluster(group));
            state.put("available", clusterManager.hasHealthyCluster(group));
            state.put("healthStatuses", clusterManager.getHealthStatuses(group));
            groups.put(group, state);
        }

        Map<String, Object> roundTrip = new LinkedHashMap<>();
        roundTrip.put("bridgedToAnalytics", bridge.bridgedCount());
        roundTrip.put("scoredInAnalytics", scoring.scoredCount());
        roundTrip.put("completedInCore", sink.completedCount());

        Map<String, Object> auditCounts = new LinkedHashMap<>();
        auditCounts.put("core", audit.coreCount());
        auditCounts.put("analytics", audit.analyticsCount());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("groups", groups);
        response.put("roundTrip", roundTrip);
        response.put("audit", auditCounts);
        return ResponseEntity.ok(response);
    }
}
