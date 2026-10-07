package dev.semeshin.kafkadr.producer;

/**
 * Asks a cluster directly, for the failures that cannot tell by themselves whether the cluster is
 * gone or merely lacks the topic — and only the first is proof of a broken cluster:
 * <ul>
 *   <li>a send that timed out waiting for metadata: Kafka reports "topic not present in metadata"
 *       for a cluster that does not answer and for a topic it does not have alike;</li>
 *   <li>a send whose record expired ("Expiring N record(s)"): a producer that already knew the
 *       topic keeps it in its metadata after the topic was deleted, batches the record, and times
 *       it out just as it would for a broker that is gone.</li>
 * </ul>
 */
@FunctionalInterface
public interface ClusterReachability {

    /**
     * @param clusterId binder id of the cluster
     * @return true when the cluster answered a metadata request just now
     */
    boolean isReachable(String clusterId);

    /**
     * @return true only when the cluster answered just now and does not have the topic; false
     *         when it has it, or did not answer
     */
    default boolean lacksTopic(String clusterId, String topic) {
        return false;
    }
}
