package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.avro.PaymentEvent;
import dev.semeshin.kafkadr.avro.PaymentStatus;
import dev.semeshin.kafkadr.producer.ResilientProducer;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
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

    /** Send a plain text message (content-type: string) */
    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, Object>> send(
            @PathVariable String topic,
            @RequestParam String message,
            @RequestParam(required = false) String messageId) {

        ResilientProducer.SendResult result = producer.send(topic, message, messageId);
        return toResponse(result, topic);
    }

    /** Send a JSON payload (content-type: json) */
    @PostMapping(value = "/{topic}/json", consumes = "application/json")
    public ResponseEntity<Map<String, Object>> sendJson(
            @PathVariable String topic,
            @RequestBody Object payload,
            @RequestParam(required = false) String messageId) {

        ResilientProducer.SendResult result = producer.send(topic, payload, messageId);
        return toResponse(result, topic);
    }

    /** Send raw bytes (content-type: bytes) */
    @PostMapping(value = "/{topic}/bytes", consumes = "application/octet-stream")
    public ResponseEntity<Map<String, Object>> sendBytes(
            @PathVariable String topic,
            @RequestBody byte[] payload,
            @RequestParam(required = false) String messageId) {

        ResilientProducer.SendResult result = producer.send(topic, payload, messageId);
        return toResponse(result, topic);
    }

    /** Send a test Avro PaymentEvent (content-type: native) */
    @PostMapping("/payment-events/avro")
    public ResponseEntity<Map<String, Object>> sendAvroPayment(
            @RequestParam String paymentId,
            @RequestParam String orderId,
            @RequestParam double amount,
            @RequestParam(defaultValue = "USD") String currency,
            @RequestParam(defaultValue = "PENDING") String status,
            @RequestParam(required = false) String messageId) {

        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(paymentId)
                .setOrderId(orderId)
                .setAmount(amount)
                .setCurrency(currency)
                .setStatus(PaymentStatus.valueOf(status))
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        ResilientProducer.SendResult result = producer.send("payment-events", event, messageId);
        return toResponse(result, "payment-events");
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("activeCluster", clusterManager.getActiveCluster());
        response.put("clusterPriority", clusterManager.getClustersByPriority());
        response.put("healthStatuses", clusterManager.getHealthStatuses());
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
