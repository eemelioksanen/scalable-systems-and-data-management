package fi.aalto.cse4780.replay;

import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Holds each row until its {@link SourceClock} time, divided by the rate factor, has elapsed since
 * pacing started. Pacing starts with the first row whose clock reaches {@code paceFrom}; earlier
 * rows are sent at full speed, which skips the quiet pre-open hours in a real-time run.
 *
 * <p>Records how far behind schedule each paced row was sent. A lag that grows means the replayer,
 * not the system under test, is limiting the rate.
 *
 * <p>Waits end early when {@code stop} becomes true, so a signal does not wait out a long gap.
 */
final class Pacer {

    /** Lags are counted per millisecond up to this bound, and in one overflow bucket beyond it. */
    private static final int MAX_TRACKED_LAG_MS = 60_000;
    /** Longest single sleep, so that {@code stop} is noticed promptly. */
    private static final long MAX_SLEEP_NANOS = 100_000_000L;

    private final double rate;
    private final long paceFrom;
    private final LongSupplier nanoTime;
    private final LongConsumer sleeper;
    private final BooleanSupplier stop;

    private long anchorClock = SourceClock.UNKNOWN;
    private long anchorNanos;
    private long lastClock = SourceClock.UNKNOWN;
    private long lastPacedNanos;

    private final long[] lagCounts = new long[MAX_TRACKED_LAG_MS + 2];
    private long paced;
    private long maxLagMs;

    /**
     * @param rate     source milliseconds per wall-clock millisecond, e.g. 10 for ten times real time
     * @param paceFrom source time at which pacing starts, or {@link SourceClock#UNKNOWN} for the first row
     */
    Pacer(double rate, long paceFrom, LongSupplier nanoTime, LongConsumer sleeper, BooleanSupplier stop) {
        if (!(rate > 0) || Double.isInfinite(rate)) {
            throw new IllegalArgumentException("rate must be positive: " + rate);
        }
        this.rate = rate;
        this.paceFrom = paceFrom;
        this.nanoTime = nanoTime;
        this.sleeper = sleeper;
        this.stop = stop;
    }

    /** Blocks until a row at source time {@code clock} is due. */
    void await(long clock) {
        if (clock == SourceClock.UNKNOWN) {
            return;
        }
        lastClock = clock;
        if (anchorClock == SourceClock.UNKNOWN) {
            if (!startsPacing(clock)) {
                return;
            }
            anchorClock = clock;
            anchorNanos = nanoTime.getAsLong();
        }
        long due = anchorNanos + (long) ((clock - anchorClock) * 1_000_000d / rate);
        long now = nanoTime.getAsLong();
        while (now < due) {
            if (stop.getAsBoolean()) {
                return;
            }
            sleeper.accept(Math.min(due - now, MAX_SLEEP_NANOS));
            now = nanoTime.getAsLong();
        }
        long lagMs = (now - due) / 1_000_000;
        lagCounts[(int) Math.min(lagMs, MAX_TRACKED_LAG_MS + 1)]++;
        maxLagMs = Math.max(maxLagMs, lagMs);
        paced++;
        lastPacedNanos = now;
    }

    /** Whether a row at {@code clock} is the first to be paced. */
    boolean startsPacing(long clock) {
        return anchorClock == SourceClock.UNKNOWN
                && clock != SourceClock.UNKNOWN
                && (paceFrom == SourceClock.UNKNOWN || clock >= paceFrom);
    }

    /** Source time at which pacing started, or {@link SourceClock#UNKNOWN} if it has not. */
    long anchorClock() {
        return anchorClock;
    }

    long lastClock() {
        return lastClock;
    }

    long paced() {
        return paced;
    }

    /** Source time covered per wall-clock time over the paced rows, or NaN before any has elapsed. */
    double achievedRate() {
        long wallNanos = lastPacedNanos - anchorNanos;
        if (paced == 0 || wallNanos <= 0) {
            return Double.NaN;
        }
        return (lastClock - anchorClock) * 1_000_000d / wallNanos;
    }

    long maxLagMs() {
        return maxLagMs;
    }

    /** The lag at quantile {@code q} over paced rows, in whole milliseconds; capped at one minute. */
    long lagQuantileMs(double q) {
        if (paced == 0) {
            return 0;
        }
        long rank = (long) Math.ceil(q * paced);
        long seen = 0;
        for (int ms = 0; ms < lagCounts.length; ms++) {
            seen += lagCounts[ms];
            if (seen >= rank) {
                return ms;
            }
        }
        return MAX_TRACKED_LAG_MS + 1;
    }
}
