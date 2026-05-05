package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.producer.ResilientProducer;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import dev.semeshin.kafkadr.routing.FailoverStateStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/messages")
public class EventController {

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;
    private final FailoverStateStore failoverStateStore;

    public EventController(ResilientProducer producer,
                           ActiveClusterManager clusterManager,
                           FailoverStateStore failoverStateStore) {
        this.producer = producer;
        this.clusterManager = clusterManager;
        this.failoverStateStore = failoverStateStore;
    }

    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, Object>> send(
            @PathVariable String topic,
            @RequestParam String message,
            @RequestParam(required = false) String messageId) {
        var result = producer.send(topic, message, messageId);
        return toResponse(result, topic);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("activeCluster", clusterManager.getActiveCluster());
        response.put("clusterPriority", clusterManager.getClustersByPriority());
        response.put("healthStatuses", clusterManager.getHealthStatuses());

        Optional<FailoverStateStore.FailoverState> persisted = failoverStateStore.load();
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("present", persisted.isPresent());
        persisted.ifPresent(s -> {
            state.put("activeCluster", s.activeCluster());
            state.put("failoverAt", s.failoverAt().toString());
        });
        response.put("persistedFailoverState", state);
        return ResponseEntity.ok(response);
    }

    private ResponseEntity<Map<String, Object>> toResponse(
            ResilientProducer.SendResult result, String topic) {
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
}
