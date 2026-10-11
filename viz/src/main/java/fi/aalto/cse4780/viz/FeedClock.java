package fi.aalto.cse4780.viz;

import fi.aalto.cse4780.replay.SourceClock;
import java.time.ZoneOffset;
import java.util.List;

/**
 * How far the replay has got, from the {@code Date} and {@code Time} of the rows at the tail of
 * {@code ticks}. It follows the replayer's source-clock rule (docs/replayer.md): the minimum over a
 * window of recent rows, never moving backward, so the stale times of each day file's opening
 * snapshot cannot pull it forward. The window is one look at the tail rather than 256 consecutive
 * rows, because the tail is sampled: 256 sampled rows would span many seconds and lag the feed.
 */
final class FeedClock {

    /** Moving less often than this counts as idle. */
    static final long IDLE_AFTER_MS = 2_000L;

    /** A {@code ticks} row's system date and time. */
    record Row(String date, String time) {}

    private final SourceClock parser = new SourceClock();
    private final long offsetMs;
    private long value = SourceClock.UNKNOWN;
    private long lastAdvanceWallMs;

    FeedClock(ZoneOffset zoneOffset) {
        this.offsetMs = zoneOffset.getTotalSeconds() * 1000L;
    }

    /** One look at the tail: the rows read since the previous look. */
    synchronized void observe(List<Row> rows, long wallMs) {
        long min = Long.MAX_VALUE;
        for (Row row : rows) {
            long t = parser.epochMillis(row.date(), row.time());
            if (t != SourceClock.UNKNOWN && t < min) {
                min = t;
            }
        }
        if (min != Long.MAX_VALUE && (value == SourceClock.UNKNOWN || min > value)) {
            value = min;
            lastAdvanceWallMs = wallMs;
        }
    }

    /**
     * Epoch ms, comparable with {@code windowEnd}: the local fields are read as if they were UTC, so
     * the zone offset is subtracted. Null before the first row.
     */
    synchronized Long feedMs() {
        return value == SourceClock.UNKNOWN ? null : value - offsetMs;
    }

    /** Wall-clock time of the last advance, if the clock has not moved for {@link #IDLE_AFTER_MS}. */
    synchronized Long idleSince(long wallMs) {
        if (value == SourceClock.UNKNOWN || wallMs - lastAdvanceWallMs < IDLE_AFTER_MS) {
            return null;
        }
        return lastAdvanceWallMs;
    }
}
