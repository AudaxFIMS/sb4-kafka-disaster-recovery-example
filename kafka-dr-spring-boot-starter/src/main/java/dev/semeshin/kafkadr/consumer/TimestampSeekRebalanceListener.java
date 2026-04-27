package dev.semeshin.kafkadr.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * On partition assignment, seeks the consumer to the offset matching the last processed
 * timestamp from the previous cluster. This avoids reprocessing already-handled messages
 * when cross-cluster replication (e.g. MirrorMaker 2) is active.
 *
 * Works together with IdempotentConsumer which provides additional deduplication
 * for messages in the timestamp boundary window.
 */
public class TimestampSeekRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(TimestampSeekRebalanceListener.class);

    private final LastProcessedTimestampTracker tracker;

    public TimestampSeekRebalanceListener(LastProcessedTimestampTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();

        for (TopicPartition tp : partitions) {
            Long lastTimestamp = tracker.getLastTimestamp(tp.topic());
            if (lastTimestamp != null) {
                timestampsToSearch.put(tp, lastTimestamp);
            }
        }

        if (timestampsToSearch.isEmpty()) return;

        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();

            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
                log.info("DR_EVENT [{}] Seeked partition {} to offset {} (timestamp={})",
                        tp.topic(), tp.partition(), offsetAndTimestamp.offset(), offsetAndTimestamp.timestamp());
            } else {
                log.info("DR_EVENT [{}] No offset found for timestamp on partition {}, using default",
                        tp.topic(), tp.partition());
            }
        }
    }
}
