package fi.aalto.cse4780.viz;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads one topic from the earliest offset and hands every record to a handler. There is no
 * consumer group: a restarted dashboard rebuilds its state from the topics.
 */
final class TopicReader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TopicReader.class);

    private final Supplier<Consumer<String, String>> consumerFactory;
    private Consumer<String, String> consumer;
    private final String topic;
    private final Predicate<ConsumerRecord<String, String>> handler;
    private final AtomicLong read = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();
    private volatile boolean running = true;
    private boolean assigned;
    private boolean warnedMissing;

    /** The handler returns false for a malformed record, which is counted and skipped. */
    TopicReader(Supplier<Consumer<String, String>> consumerFactory, String topic,
            Predicate<ConsumerRecord<String, String>> handler) {
        this.consumerFactory = consumerFactory;
        this.topic = topic;
        this.handler = handler;
    }

    /** {@code ema-results} into the store. */
    static TopicReader results(Supplier<Consumer<String, String>> factory, String topic, Store store) {
        return new TopicReader(factory, topic, record -> {
            Result r = Result.parse(record.value());
            if (r != null) {
                store.add(r);
            }
            return r != null;
        });
    }

    /** {@code advisories} into the store. */
    static TopicReader advisories(Supplier<Consumer<String, String>> factory, String topic, Store store) {
        return new TopicReader(factory, topic, record -> {
            Advisory a = Advisory.parse(record.value());
            if (a != null) {
                store.addAdvisory(a);
            }
            return a != null;
        });
    }

    /** {@code subscriptions}, keyed by trader, into the current lists. */
    static TopicReader subscriptions(Supplier<Consumer<String, String>> factory, String topic, Subscriptions subs) {
        return new TopicReader(factory, topic, record -> subs.apply(record.key(), record.value()));
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
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    /** One poll. Returns false while the topic does not exist yet. */
    boolean pollOnce(Duration timeout) {
        if (consumer == null) {
            // Created here, not at startup, so an unresolvable broker is retried rather than fatal.
            consumer = consumerFactory.get();
        }
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
            if (handler.test(record)) {
                read.incrementAndGet();
            } else {
                malformed.incrementAndGet();
            }
        }
        return true;
    }

    String topic() {
        return topic;
    }

    long read() {
        return read.get();
    }

    long malformed() {
        return malformed.get();
    }

    void stop() {
        running = false;
        Consumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup();
        }
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
