package dev.semeshin.kafkadr.consumer;

import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;

/**
 * Delegating {@link Acknowledgment} that remembers whether it was used.
 *
 * <p>Nothing else can observe the commit: the container hands the handler an
 * {@code Acknowledgment} and never reports back. Without this the starter would advance the
 * seek-by-timestamp watermark for records the handler silently left uncommitted, and a
 * forgotten {@code acknowledge()} would surface only as a consumer group that stops
 * committing — long after the code that caused it.
 *
 * <p>The flags are volatile because {@code ack.async-acks=true} allows the handler to
 * acknowledge from another thread.
 */
final class TrackingAcknowledgment implements Acknowledgment {

    private final Acknowledgment delegate;
    private volatile boolean acknowledged;
    private volatile boolean nacked;

    TrackingAcknowledgment(Acknowledgment delegate) {
        this.delegate = delegate;
    }

    // The flag is set after the delegate call on purpose: an unsupported or rejected
    // acknowledgment must not count as a commit the watermark can follow.

    @Override
    public void acknowledge() {
        this.delegate.acknowledge();
        this.acknowledged = true;
    }

    @Override
    public void acknowledge(int index) {
        this.delegate.acknowledge(index);
        this.acknowledged = true;
    }

    @Override
    public void nack(Duration sleep) {
        this.delegate.nack(sleep);
        this.nacked = true;
    }

    @Override
    public void nack(int index, Duration sleep) {
        this.delegate.nack(index, sleep);
        this.nacked = true;
    }

    @Override
    public boolean isOutOfOrderCommit() {
        return this.delegate.isOutOfOrderCommit();
    }

    boolean isAcknowledged() {
        return this.acknowledged;
    }

    /** A deliberate redelivery request, which is not a forgotten acknowledgment. */
    boolean isNacked() {
        return this.nacked;
    }
}
