package fi.aalto.cse4780.viz;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@code ema-results} from the earliest offset into the {@link Store}. There is no consumer
 * group: a restarted dashboard rebuilds its state from the topic.
 */
final class ResultsConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ResultsConsumer.class);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final Store store;
    private final AtomicLong resultsRead = new AtomicLong();
    private final AtomicLong malformedResults = new AtomicLong();
    private volatile boolean running = true;
    private boolean assigned;
    private boolean warnedMissing;

    ResultsConsumer(Consumer<String, String> consumer, String topic, Store store) {
        this.consumer = consumer;
        this.topic = topic;
        this.store = store;
    }

    @Override
    public void run() {
        try {
            while (running) {
                try {
                    if (!pollOnce(Duration.ofMillis(500))) {
                        sleep(5_000);
                    }
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    break;
                } catch (RuntimeException e) {
                    LOG.warn("Reading {} failed; retrying", topic, e);
                    sleep(1_000);
                }
            }
        } finally {
            consumer.close();
        }
    }

    /** One poll. Returns false while the topic does not exist yet. */
    boolean pollOnce(Duration timeout) {
        if (!assigned) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(5));
            if (partitions == null || partitions.isEmpty()) {
                if (!warnedMissing) {
                    LOG.warn("Topic {} does not exist yet; waiting for it", topic);
                    warnedMissing = true;
                }
                return false;
            }
            List<TopicPartition> tps = partitions.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            assigned = true;
            LOG.info("Reading {} partitions of {} from the earliest offset", tps.size(), topic);
        }
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
            Result result = Result.parse(record.value());
            if (result == null) {
                malformedResults.incrementAndGet();
                continue;
            }
            resultsRead.incrementAndGet();
            store.add(result);
        }
        return true;
    }

    long resultsRead() {
        return resultsRead.get();
    }

    long malformedResults() {
        return malformedResults.get();
    }

    void stop() {
        running = false;
        consumer.wakeup();
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
