package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.handler.AuditRecordProcessor;
import dev.semeshin.kafkadr.handler.OrderBatchProcessor;
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
public class MixedBatchController {

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;
    private final OrderBatchProcessor batchProcessor;
    private final AuditRecordProcessor recordProcessor;

    public MixedBatchController(ResilientProducer producer,
                                ActiveClusterManager clusterManager,
                                OrderBatchProcessor batchProcessor,
                                AuditRecordProcessor recordProcessor) {
        this.producer = producer;
        this.clusterManager = clusterManager;
        this.batchProcessor = batchProcessor;
        this.recordProcessor = recordProcessor;
    }

    /**
     * Publishes orders through {@code sendBatch}, which makes one failover decision for the
     * whole batch instead of re-running the retry ladder per message.
     *
     * @param count    how many orders to generate
     * @param customer set to {@code flaky} to have the consumer ask for redelivery, or
     *                 leave empty for orders that process normally
     * @param amount   negative to have the consumer discard the orders as unprocessable
     */
    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> sendOrders(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "acme") String customer,
            @RequestParam(defaultValue = "100") int amount) {

        List<Message<?>> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            OrderEvent order = new OrderEvent(UUID.randomUUID().toString(), amount, customer);
            messages.add(MessageBuilder.withPayload(order)
                    .setHeader(KafkaHeaders.KEY, order.orderId())
                    .build());
        }

        ResilientProducer.BatchSendResult result = producer.sendBatch("order-events", messages);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("requested", result.size());
        response.put("sent", result.sent());
        response.put("failed", result.failed());
        // More than one cluster means the batch failed over part-way through.
        response.put("clusters", result.clusters());
        return result.allSent()
                ? ResponseEntity.ok(response)
                : ResponseEntity.internalServerError().body(response);
    }

    /** Publishes one audit record, consumed the ordinary way. */
    @PostMapping("/audit")
    public ResponseEntity<Map<String, Object>> sendAudit(
            @RequestParam String message,
            @RequestParam(required = false) String messageId) {

        ResilientProducer.SendResult result = producer.send("audit-events", message, messageId);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", result.success() ? "sent" : "failed");
        response.put("cluster", result.cluster() != null ? result.cluster() : "none");
        response.put("messageId", result.messageId());
        return result.success()
                ? ResponseEntity.ok(response)
                : ResponseEntity.internalServerError().body(response);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("activeCluster", clusterManager.getActiveCluster());
        response.put("clusterPriority", clusterManager.getClustersByPriority());
        response.put("healthStatuses", clusterManager.getHealthStatuses());
        response.put("orderBatches", batchProcessor.batchCount());
        response.put("ordersProcessed", batchProcessor.recordCount());
        response.put("auditRecordsProcessed", recordProcessor.processedCount());
        return ResponseEntity.ok(response);
    }
}
