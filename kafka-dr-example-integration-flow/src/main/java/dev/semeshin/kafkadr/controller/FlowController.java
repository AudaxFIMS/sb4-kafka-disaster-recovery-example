package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.flow.FlowMetrics;
import dev.semeshin.kafkadr.handler.BillingBatchProcessor;
import dev.semeshin.kafkadr.handler.InvoiceProcessor;
import dev.semeshin.kafkadr.handler.OrderFlowProcessor;
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
public class FlowController {

    private final ResilientProducer producer;
    private final ActiveClusterManager clusterManager;
    private final OrderFlowProcessor orderProcessor;
    private final BillingBatchProcessor billingProcessor;
    private final InvoiceProcessor invoiceProcessor;
    private final FlowMetrics metrics;

    public FlowController(ResilientProducer producer,
                          ActiveClusterManager clusterManager,
                          OrderFlowProcessor orderProcessor,
                          BillingBatchProcessor billingProcessor,
                          InvoiceProcessor invoiceProcessor,
                          FlowMetrics metrics) {
        this.producer = producer;
        this.clusterManager = clusterManager;
        this.orderProcessor = orderProcessor;
        this.billingProcessor = billingProcessor;
        this.invoiceProcessor = invoiceProcessor;
        this.metrics = metrics;
    }

    /**
     * Feeds the record path. {@code amount=0} is filtered out by the flow and still counts as
     * consumed; a positive amount comes back as an invoice on {@code flow-invoices}.
     */
    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> sendOrders(
            @RequestParam(defaultValue = "5") int count,
            @RequestParam(defaultValue = "100") int amount,
            @RequestParam(defaultValue = "acme") String customer) {

        int sent = 0;
        for (int i = 0; i < count; i++) {
            OrderEvent order = new OrderEvent(UUID.randomUUID().toString(), amount, customer);
            if (producer.send("flow-orders", order, order.orderId()).success()) {
                sent++;
            }
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("requested", count);
        response.put("sent", sent);
        return sent == count ? ResponseEntity.ok(response) : ResponseEntity.internalServerError().body(response);
    }

    /**
     * Feeds the batch path. A negative amount makes the batch handler discard that record
     * through the typed catch the gateway makes possible.
     */
    @PostMapping("/billing")
    public ResponseEntity<Map<String, Object>> sendBilling(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "100") int amount,
            @RequestParam(defaultValue = "acme") String customer) {

        List<Message<?>> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            OrderEvent order = new OrderEvent(UUID.randomUUID().toString(), amount, customer);
            messages.add(MessageBuilder.withPayload(order)
                    .setHeader(KafkaHeaders.KEY, order.orderId())
                    .build());
        }

        ResilientProducer.BatchSendResult result = producer.sendBatch("flow-billing", messages);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("requested", result.size());
        response.put("sent", result.sent());
        response.put("failed", result.failed());
        response.put("clusters", result.clusters());
        return result.allSent() ? ResponseEntity.ok(response)
                : ResponseEntity.internalServerError().body(response);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("activeCluster", clusterManager.getActiveCluster());
        response.put("healthStatuses", clusterManager.getHealthStatuses());

        Map<String, Object> recordPath = new LinkedHashMap<>();
        recordPath.put("ordersConsumed", orderProcessor.receivedCount());
        recordPath.put("enteredFlow", metrics.getEntered());
        recordPath.put("filteredOut", metrics.getFiltered());
        recordPath.put("invoicesPublished", metrics.getInvoicesPublished());
        response.put("recordPath", recordPath);

        Map<String, Object> batchPath = new LinkedHashMap<>();
        batchPath.put("batches", billingProcessor.batchCount());
        batchPath.put("billed", billingProcessor.billedCount());
        batchPath.put("discarded", billingProcessor.discardedCount());
        response.put("batchPath", batchPath);

        response.put("invoicesConsumed", invoiceProcessor.consumedCount());
        return ResponseEntity.ok(response);
    }
}
