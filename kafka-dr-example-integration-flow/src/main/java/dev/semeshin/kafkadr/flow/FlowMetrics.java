package dev.semeshin.kafkadr.flow;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/** Counters the flows bump, so {@code GET /api/status} can show what actually happened. */
@Component
public class FlowMetrics {

    private final AtomicInteger entered = new AtomicInteger();
    private final AtomicInteger filtered = new AtomicInteger();
    private final AtomicInteger invoicesPublished = new AtomicInteger();

    public void entered() { entered.incrementAndGet(); }
    public void filtered() { filtered.incrementAndGet(); }
    public void invoicePublished() { invoicesPublished.incrementAndGet(); }

    public int getEntered() { return entered.get(); }
    public int getFiltered() { return filtered.get(); }
    public int getInvoicesPublished() { return invoicesPublished.get(); }
}
