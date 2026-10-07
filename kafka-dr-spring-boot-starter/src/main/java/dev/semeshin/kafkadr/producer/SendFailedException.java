package dev.semeshin.kafkadr.producer;

/**
 * A send through {@link ResilientProducer} did not reach Kafka, raised by
 * {@link ResilientProducer.SendResult#orThrow()} for callers that would rather propagate than
 * inspect the result — a handler bridging two Kafkas, for one, whose exception is what makes
 * the source record come back instead of being committed.
 */
public class SendFailedException extends RuntimeException {

    private final String group;
    private final String messageId;
    private final ResilientProducer.Failure failure;

    public SendFailedException(String message, String group, String messageId, ResilientProducer.Failure failure) {
        super(message);
        this.group = group;
        this.messageId = messageId;
        this.failure = failure;
    }

    /** Cluster group the send was addressed to. */
    public String getGroup() { return group; }

    /** Key of the message that was not sent; null for a batch. */
    public String getMessageId() { return messageId; }

    public ResilientProducer.Failure getFailure() { return failure; }
}
