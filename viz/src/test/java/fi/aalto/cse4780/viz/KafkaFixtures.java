package fi.aalto.cse4780.viz;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/** MockConsumer set-up shared by the consumer tests. */
final class KafkaFixtures {

    private KafkaFixtures() {}

    static MockConsumer<String, String> consumerWithTopic(String topic, int partitions) {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> infos = new java.util.ArrayList<>();
        Map<TopicPartition, Long> zero = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            infos.add(new PartitionInfo(topic, p, node, new Node[] {node}, new Node[] {node}));
            zero.put(new TopicPartition(topic, p), 0L);
        }
        consumer.updatePartitions(topic, infos);
        consumer.updateBeginningOffsets(zero);
        consumer.updateEndOffsets(zero);
        return consumer;
    }

    static <T> java.util.function.Supplier<T> supply(T value) {
        return () -> value;
    }

    static ConsumerRecord<String, String> record(String topic, int partition, long offset, long timestamp, String value) {
        return record(topic, partition, offset, timestamp, "k", value);
    }

    static ConsumerRecord<String, String> record(String topic, int partition, long offset, long timestamp,
            String key, String value) {
        return new ConsumerRecord<>(topic, partition, offset, timestamp,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, key, value,
                new org.apache.kafka.common.header.internals.RecordHeaders(), java.util.Optional.empty());
    }
}
