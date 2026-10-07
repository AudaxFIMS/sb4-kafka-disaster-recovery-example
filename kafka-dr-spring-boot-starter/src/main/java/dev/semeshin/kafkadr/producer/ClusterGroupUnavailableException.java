package dev.semeshin.kafkadr.producer;

/**
 * The send failed because no cluster of the target group could take it — not because of the
 * message. Retrying the same message later is the right response, unlike for a
 * serialization error, which would fail again on any cluster.
 */
public class ClusterGroupUnavailableException extends SendFailedException {

    public ClusterGroupUnavailableException(String message, String group, String messageId,
                                            ResilientProducer.Failure failure) {
        super(message, group, messageId, failure);
    }
}
