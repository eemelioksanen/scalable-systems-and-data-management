package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.SourceClock.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class SourceClockTest {

    private final SourceClock clock = new SourceClock();
    private final CsvLine line = new CsvLine();

    static long at(String localDateTime) {
        return LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private long systemTime(String date, String time) {
        line.parse(TestCsv.row("AIR.FR", date, time, "", "", ""));
        return clock.epochMillis(line);
    }

    @Test
    void readsTheSystemDateAndTime() {
        assertEquals(at("2021-11-08T09:00:01.123"), systemTime("08-11-2021", "09:00:01.123"));
        assertEquals(at("2021-11-09T00:42:58"), systemTime("09-11-2021", "00:42:58.000"));
        assertEquals(at("2021-11-08T09:00:01"), systemTime("08-11-2021", "09:00:01"));
        assertEquals(at("2021-11-08T09:00:01.100"), systemTime("08-11-2021", "09:00:01.1"));
        assertEquals(at("2021-11-08T09:00:01.123"), systemTime("08-11-2021", "09:00:01.123456"));
    }

    @Test
    void treatsMissingOrMalformedTimesAsUnknown() {
        assertEquals(UNKNOWN, systemTime("", ""));
        assertEquals(UNKNOWN, systemTime("08-11-2021", ""));
        assertEquals(UNKNOWN, systemTime("", "09:00:00.000"));
        assertEquals(UNKNOWN, systemTime("31-02-2021", "09:00:00.000"));
        assertEquals(UNKNOWN, systemTime("08-11-2021", "24:00:00.000"));
        assertEquals(UNKNOWN, systemTime("08-11-2021", "09:00"));
    }

    @Test
    void followsAnOrderedStreamOneWindowBehind() {
        long start = at("2021-11-08T09:00");
        for (int i = 0; i < 1000; i++) {
            long now = clock.update(start + i);
            assertEquals(start + Math.max(0, i - SourceClock.WINDOW + 1), now);
        }
    }

    @Test
    void isNotPulledForwardByRowsStampedAhead() {
        long start = at("2021-11-08T01:01:07");
        clock.update(start);
        clock.update(at("2021-11-08T18:30:34"));
        for (int i = 1; i < 10; i++) {
            clock.update(start + i);
        }
        assertEquals(start, clock.now());
    }

    @Test
    void isNotPulledBackByStaleRows() {
        long start = at("2021-11-08T17:30");
        for (int i = 0; i < 2 * SourceClock.WINDOW; i++) {
            clock.update(start + i);
        }
        long before = clock.now();
        for (int i = 0; i < 3 * SourceClock.WINDOW; i++) {
            clock.update(at("2021-11-08T00:00"));
        }
        assertEquals(before, clock.now());
    }

    @Test
    void jumpsAGapOnceAWindowOfRowsIsPastIt() {
        long evening = at("2021-11-08T22:25");
        long morning = at("2021-11-09T01:01");
        clock.update(evening);
        for (int i = 1; i < SourceClock.WINDOW; i++) {
            clock.update(morning + i);
            assertEquals(evening, clock.now());
        }
        clock.update(morning + SourceClock.WINDOW);
        assertEquals(morning + 1, clock.now());
    }

    @Test
    void ignoresRowsWithoutATime() {
        assertEquals(UNKNOWN, clock.update(UNKNOWN));
        clock.update(at("2021-11-08T09:00"));
        assertEquals(at("2021-11-08T09:00"), clock.update(UNKNOWN));
    }
}
