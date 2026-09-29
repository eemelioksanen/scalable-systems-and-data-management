package fi.aalto.cse4780.replay;

import java.time.DateTimeException;
import java.time.LocalDate;

/**
 * The source clock that paces live replay: a non-decreasing estimate of when the feed delivered
 * each row, built from the system {@code Date} and {@code Time} columns ("system time for last
 * received update").
 *
 * <p>The raw column cannot pace replay by itself. Each day file opens with a snapshot of about
 * 100,000 rows stamped with stale times from across the whole day, including the previous close
 * (17:35) and midnight, and closes with a similar dump. A running maximum would jump to the evening
 * within the first 200 rows and send the rest of the day unpaced; a running median follows the
 * snapshot's 17:35 block. Between the two dumps the column is ordered to within a second.
 *
 * <p>The clock is therefore the minimum over the last {@value #WINDOW} rows that carry a time, and
 * it never moves backward. Rows stamped later than their neighbours cannot pull it forward, and
 * stale rows only hold it. On the 08.11.2021 file it stays within one second of 99% of rows' own
 * times and paces the opening snapshot over 01:01-07:15 (docs/replayer.md).
 *
 * <p>Times are local wall-clock fields read as UTC epoch milliseconds. Only differences are used, so
 * the timezone question of D-3 does not arise.
 */
final class SourceClock {

    static final int WINDOW = 256;
    static final long UNKNOWN = Long.MIN_VALUE;

    private static final long MILLIS_PER_DAY = 86_400_000L;

    // Monotonic deque over the window: row indices with strictly increasing times, oldest first.
    private final long[] dequeIndex = new long[WINDOW];
    private final long[] dequeTime = new long[WINDOW];
    private int head;
    private int size;
    private long rows;

    private long now = UNKNOWN;

    private String cachedDate;
    private long cachedEpochDay;

    /** Advances the clock with the {@code Date} and {@code Time} fields of a data row. */
    long update(CsvLine row) {
        return update(epochMillis(row));
    }

    /** Advances the clock with one row's time, or leaves it unchanged for {@link #UNKNOWN}. */
    long update(long rowTime) {
        if (rowTime == UNKNOWN) {
            return now;
        }
        long index = rows++;
        if (size > 0 && dequeIndex[head] <= index - WINDOW) {
            head = (head + 1) % WINDOW;
            size--;
        }
        while (size > 0 && dequeTime[(head + size - 1) % WINDOW] >= rowTime) {
            size--;
        }
        int tail = (head + size) % WINDOW;
        dequeIndex[tail] = index;
        dequeTime[tail] = rowTime;
        size++;
        long windowMin = dequeTime[head];
        if (now == UNKNOWN || windowMin > now) {
            now = windowMin;
        }
        return now;
    }

    long now() {
        return now;
    }

    /** {@code Date} ({@code DD-MM-YYYY}) plus {@code Time} ({@code HH:MM:SS[.fff]}), or {@link #UNKNOWN}. */
    long epochMillis(CsvLine row) {
        String line = row.line();
        long epochDay = epochDay(line, row.start(CsvLine.DATE), row.end(CsvLine.DATE));
        long millisOfDay = millisOfDay(line, row.start(CsvLine.TIME), row.end(CsvLine.TIME));
        if (epochDay == UNKNOWN || millisOfDay == UNKNOWN) {
            return UNKNOWN;
        }
        return epochDay * MILLIS_PER_DAY + millisOfDay;
    }

    private long epochDay(String line, int from, int to) {
        if (to - from != 10) {
            return UNKNOWN;
        }
        if (cachedDate != null && line.regionMatches(from, cachedDate, 0, 10)) {
            return cachedEpochDay;
        }
        int day = digits(line, from, from + 2);
        int month = digits(line, from + 3, from + 5);
        int year = digits(line, from + 6, from + 10);
        if (day < 0 || month < 0 || year < 0 || line.charAt(from + 2) != '-' || line.charAt(from + 5) != '-') {
            return UNKNOWN;
        }
        try {
            cachedEpochDay = LocalDate.of(year, month, day).toEpochDay();
        } catch (DateTimeException e) {
            return UNKNOWN;
        }
        cachedDate = line.substring(from, to);
        return cachedEpochDay;
    }

    /** Milliseconds since midnight. Fraction digits beyond milliseconds are truncated (D-22). */
    static long millisOfDay(String line, int from, int to) {
        if (to - from < 8 || line.charAt(from + 2) != ':' || line.charAt(from + 5) != ':') {
            return UNKNOWN;
        }
        int hours = digits(line, from, from + 2);
        int minutes = digits(line, from + 3, from + 5);
        int seconds = digits(line, from + 6, from + 8);
        if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59) {
            return UNKNOWN;
        }
        long millis = 0;
        if (to - from > 8) {
            if (line.charAt(from + 8) != '.') {
                return UNKNOWN;
            }
            int fractionEnd = Math.min(to, from + 12);
            int fraction = digits(line, from + 9, fractionEnd);
            if (fraction < 0 || fractionEnd == from + 9) {
                return UNKNOWN;
            }
            millis = fraction;
            for (int i = fractionEnd - (from + 9); i < 3; i++) {
                millis *= 10;
            }
        }
        return ((hours * 60L + minutes) * 60L + seconds) * 1000L + millis;
    }

    /** {@code s[from, to)} as a non-negative decimal, or -1. */
    private static int digits(String s, int from, int to) {
        int value = 0;
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            value = value * 10 + (c - '0');
        }
        return value;
    }
}
