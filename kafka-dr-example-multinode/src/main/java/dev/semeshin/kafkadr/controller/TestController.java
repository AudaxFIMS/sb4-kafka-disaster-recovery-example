package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.producer.ResilientProducer;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import dev.semeshin.kafkadr.health.ClusterHealthChecker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/messages")
public class TestController {

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;

    public TestController(ResilientProducer producer, ActiveClusterManager clusterManager) {
        this.producer = producer;
        this.clusterManager = clusterManager;
    }

    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, Object>> send(
            @PathVariable String topic,
            @RequestParam String message,
            @RequestParam(required = false) String messageId) {
        var result = producer.send(topic, message, messageId);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", result.success() ? "sent" : "failed");
        response.put("cluster", result.cluster() != null ? result.cluster() : "none");
        response.put("topic", topic);
        response.put("messageId", result.messageId());
        if (result.success()) {
            return ResponseEntity.ok(response);
        }
        return ResponseEntity.internalServerError().body(response);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("activeCluster", clusterManager.getActiveCluster());
        response.put("clusterPriority", clusterManager.getClustersByPriority());
        response.put("healthStatuses", clusterManager.getHealthStatuses());
        return ResponseEntity.ok(response);
    }
}
