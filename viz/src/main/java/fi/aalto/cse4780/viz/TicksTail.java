package fi.aalto.cse4780.viz;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Once a second, reads the rows added to {@code ticks} since the last look, at most the last
 * {@value #TAIL_PER_PARTITION} per partition, and feeds them to the {@link FeedClock}.
 */
final class TicksTail implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TicksTail.class);

    static final int TAIL_PER_PARTITION = 32;
    private static final int DATE = 2;
    private static final int TIME = 3;

    private final Consumer<String, String> consumer;
    private final String topic;
    private final FeedClock clock;
    private final LongSupplier wallClock;
    private final Map<TopicPartition, Long> seen = new HashMap<>();
    private List<TopicPartition> partitions;
    private volatile boolean running = true;

    TicksTail(Consumer<String, String> consumer, String topic, FeedClock clock, LongSupplier wallClock) {
        this.consumer = consumer;
        this.topic = topic;
        this.clock = clock;
        this.wallClock = wallClock;
    }

    @Override
    public void run() {
        try {
            while (running) {
                try {
                    if (!pollOnce()) {
                        sleep(5_000);
                        continue;
                    }
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    break;
                } catch (RuntimeException e) {
                    LOG.warn("Reading the tail of {} failed; retrying", topic, e);
                }
                sleep(1_000);
            }
        } finally {
            consumer.close();
        }
    }

    /** One look at the tail. Returns false while the topic does not exist yet. */
    boolean pollOnce() {
        if (partitions == null) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
            if (infos == null || infos.isEmpty()) {
                return false;
            }
            partitions = infos.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
        }
        Map<TopicPartition, Long> ends = consumer.endOffsets(partitions, Duration.ofSeconds(5));
        Map<TopicPartition, Long> pending = new HashMap<>();
        for (TopicPartition tp : partitions) {
            long end = ends.getOrDefault(tp, 0L);
            long last = seen.getOrDefault(tp, 0L);
            if (last > end) {
                // The topic was reset (docs/replayer.md); start again from its new tail.
                last = 0;
            }
            long from = Math.max(last, end - TAIL_PER_PARTITION);
            if (from < end) {
                consumer.seek(tp, Math.max(0, from));
                pending.put(tp, end);
            } else {
                consumer.pause(List.of(tp));
            }
        }
        if (pending.isEmpty()) {
            return true;
        }
        consumer.resume(pending.keySet());
        List<ConsumerRecord<String, String>> rows = new ArrayList<>();
        for (int emptyPolls = 0; !pending.isEmpty() && emptyPolls < 3; ) {
            var records = consumer.poll(Duration.ofMillis(200));
            if (records.isEmpty()) {
                emptyPolls++;
            }
            for (ConsumerRecord<String, String> r : records) {
                rows.add(r);
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                seen.put(tp, r.offset() + 1);
                Long end = pending.get(tp);
                if (end != null && r.offset() + 1 >= end) {
                    pending.remove(tp);
                    consumer.pause(List.of(tp));
                }
            }
        }
        List<FeedClock.Row> times = new ArrayList<>(rows.size());
        for (ConsumerRecord<String, String> r : rows) {
            String[] fields = r.value() == null ? new String[0] : r.value().split(",", -1);
            if (fields.length > TIME) {
                times.add(new FeedClock.Row(fields[DATE], fields[TIME]));
            }
        }
        clock.observe(times, wallClock.getAsLong());
        return true;
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
