package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FeedClockTest {

    private final FeedClock clock = new FeedClock(ZoneOffset.of("+02:00"));

    static long cest(String localDateTime) {
        return LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.of("+02:00")).toEpochMilli();
    }

    static List<FeedClock.Row> rows(String... times) {
        List<FeedClock.Row> out = new ArrayList<>();
        for (String t : times) {
            out.add(new FeedClock.Row("08-11-2021", t));
        }
        return out;
    }

    @Test
    void isUnknownBeforeTheFirstRow() {
        assertNull(clock.feedMs());
        assertNull(clock.idleSince(0));
        clock.observe(List.of(), 0);
        assertNull(clock.feedMs());
    }

    @Test
    void appliesTheZoneOffsetSoFeedTimeComparesWithWindowEnd() {
        clock.observe(rows("09:05:00.000"), 0);
        assertEquals(cest("2021-11-08T09:05:00"), clock.feedMs());
        assertEquals(StoreTest.T0, clock.feedMs());
    }

    @Test
    void aStaleRowStampedLaterDoesNotPullItForward() {
        clock.observe(rows("09:00:00.000", "17:35:00.000", "09:00:00.500"), 0);
        assertEquals(cest("2021-11-08T09:00:00"), clock.feedMs());
    }

    @Test
    void neverGoesBackward() {
        clock.observe(rows("09:00:00.000"), 0);
        clock.observe(rows("01:00:00.000", "09:30:00.000"), 0);
        assertEquals(cest("2021-11-08T09:00:00"), clock.feedMs());

        clock.observe(rows("09:30:00.000"), 0);
        assertEquals(cest("2021-11-08T09:30:00"), clock.feedMs());
    }

    @Test
    void ignoresRowsWithoutATime() {
        clock.observe(List.of(new FeedClock.Row("08-11-2021", ""), new FeedClock.Row("", "")), 0);
        assertNull(clock.feedMs());
    }

    @Test
    void reportsWhenItStoppedMoving() {
        clock.observe(rows("09:00:00.000"), 1_000);
        assertNull(clock.idleSince(1_500));
        assertEquals(1_000L, clock.idleSince(1_000 + FeedClock.IDLE_AFTER_MS));
    }
}
