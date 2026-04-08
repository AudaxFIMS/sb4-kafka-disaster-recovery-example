package com.example.kafkadr.producer;

import com.example.kafkadr.routing.ActiveClusterManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/messages")
public class MessageProducerController {

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;

    public MessageProducerController(ResilientProducer producer,
                                     ActiveClusterManager clusterManager) {
        this.producer = producer;
        this.clusterManager = clusterManager;
    }

    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, String>> send(
            @PathVariable String topic,
            @RequestParam String message,
            @RequestParam(required = false) String messageId) {

        ResilientProducer.SendResult result = producer.send(topic, message, messageId);

        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", result.success() ? "sent" : "failed");
        response.put("cluster", result.cluster() != null ? result.cluster() : "none");
        response.put("topic", topic);
        response.put("messageId", result.messageId());
        response.put("message", message);

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
