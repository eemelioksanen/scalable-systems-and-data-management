package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.ZoneOffset;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class TicksTailTest {

    private final MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("ticks", 1);
    private final FeedClock clock = new FeedClock(ZoneOffset.of("+02:00"));
    private final TicksTail tail = new TicksTail(consumer, "ticks", clock, () -> 0L);
    private final TopicPartition p0 = new TopicPartition("ticks", 0);

    private void append(long from, long count, String time) {
        for (long o = from; o < from + count; o++) {
            consumer.addRecord(KafkaFixtures.record("ticks", 0, o, o,
                    "AIR.FR,E,08-11-2021," + time + ",,,"));
        }
        consumer.updateEndOffsets(Map.of(p0, from + count));
    }

    @Test
    void feedsTheTailOfTheTopicToTheClock() {
        tail.pollOnce();
        assertNull(clock.feedMs());

        append(0, 300, "09:00:00.000");
        tail.pollOnce();
        assertEquals(FeedClockTest.cest("2021-11-08T09:00:00"), clock.feedMs());
    }

    @Test
    void readsOnlyRowsItHasNotSeen() {
        tail.pollOnce();
        append(0, 40, "09:00:00.000");
        tail.pollOnce();
        Long first = clock.feedMs();

        tail.pollOnce();
        assertEquals(first, clock.feedMs());

        append(40, 300, "09:10:00.000");
        tail.pollOnce();
        assertEquals(FeedClockTest.cest("2021-11-08T09:10:00"), clock.feedMs());
    }

    @Test
    void waitsWhileTheTopicDoesNotExist() {
        TicksTail missing = new TicksTail(KafkaFixtures.consumerWithTopic("other", 1), "ticks", clock, () -> 0L);
        assertFalse(missing.pollOnce());
    }
}
