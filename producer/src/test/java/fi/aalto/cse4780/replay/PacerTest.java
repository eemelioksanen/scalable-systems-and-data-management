package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.SourceClockTest.at;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PacerTest {

    private static final long MS = 1_000_000L;

    /** Simulated monotonic time; sleeping advances it exactly. */
    private long nanos = 5_000 * MS;
    private final List<Long> sleeps = new ArrayList<>();
    private boolean stop;

    private Pacer pacer(double rate, long paceFrom) {
        return new Pacer(rate, paceFrom, () -> nanos, n -> {
            sleeps.add(n);
            nanos += n;
        }, () -> stop);
    }

    @Test
    void holdsRowsUntilTheirScaledSourceTime() {
        Pacer pacer = pacer(10, SourceClock.UNKNOWN);
        long start = at("2021-11-08T09:00");
        long wallStart = nanos;

        pacer.await(start);
        pacer.await(start + 1_000);
        assertEquals(wallStart + 100 * MS, nanos);
        pacer.await(start + 1_000);
        assertEquals(wallStart + 100 * MS, nanos, "rows at the same clock go together");
        pacer.await(start + 5_000);
        assertEquals(wallStart + 500 * MS, nanos);

        assertEquals(4, pacer.paced());
        assertEquals(0, pacer.maxLagMs());
        assertEquals(10.0, pacer.achievedRate(), 1e-9);
    }

    @Test
    void sleepsInSlicesSoThatStopIsNoticed() {
        Pacer pacer = pacer(1, SourceClock.UNKNOWN);
        long start = at("2021-11-08T17:30");

        pacer.await(start);
        pacer.await(start + 1_000);
        assertEquals(List.of(100 * MS, 100 * MS, 100 * MS, 100 * MS, 100 * MS,
                100 * MS, 100 * MS, 100 * MS, 100 * MS, 100 * MS), sleeps);

        sleeps.clear();
        stop = true;
        pacer.await(start + 3_600_000);
        assertTrue(sleeps.isEmpty(), "a stopped replay sends the waiting row at once");
    }

    @Test
    void sendsRowsBeforePaceFromUnpaced() {
        Pacer pacer = pacer(1, at("2021-11-08T09:00"));
        long wallStart = nanos;

        pacer.await(at("2021-11-08T01:01"));
        pacer.await(at("2021-11-08T07:15"));
        assertEquals(wallStart, nanos);
        assertEquals(SourceClock.UNKNOWN, pacer.anchorClock());
        assertFalse(pacer.startsPacing(at("2021-11-08T08:59:59.999")));
        assertTrue(pacer.startsPacing(at("2021-11-08T09:00:00.100")));

        pacer.await(at("2021-11-08T09:00:00.100"));
        pacer.await(at("2021-11-08T09:00:01.100"));
        assertEquals(wallStart + 1_000 * MS, nanos);
        assertEquals(at("2021-11-08T09:00:00.100"), pacer.anchorClock());
        assertFalse(pacer.startsPacing(at("2021-11-08T09:00:02")));
        assertEquals(2, pacer.paced());
    }

    @Test
    void measuresLagWhenTheReplayerFallsBehind() {
        Pacer pacer = pacer(1, SourceClock.UNKNOWN);
        long start = at("2021-11-08T09:00");
        pacer.await(start);
        for (int i = 1; i <= 100; i++) {
            nanos += 2 * MS;
            pacer.await(start + i);
        }
        assertEquals(0, pacer.lagQuantileMs(0.001));
        assertEquals(100, pacer.lagQuantileMs(1.0));
        assertEquals(100, pacer.maxLagMs());
        assertEquals(50, pacer.lagQuantileMs(0.5), 1);
    }
}
