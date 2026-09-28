package fi.aalto.cse4780.trading;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Parses and classifies one {@code ticks} record (docs/interfaces.md):
 *
 * <pre>ID,SecType,Date,Time,Last,Trading time,Trading date</pre>
 *
 * <p>Trading dates are {@code DD-MM-YYYY} and trading times {@code HH:MM:SS[.fraction]} with up to
 * nine fraction digits. They are local times at a fixed {@link ZoneOffset} (D-3). Any whole-hour
 * offset keeps 5-minute windows aligned to local midnight.
 *
 * <p>Not thread-safe: it caches the last parsed date. Use one instance per source reader.
 */
public final class TickParser implements Serializable {

    private static final long serialVersionUID = 1L;

    /** How a record was classified. Only {@link #PRICE} records continue into the query. */
    public enum Outcome {
        /** {@code Last} is set and the trading timestamp parses. */
        PRICE,
        /** {@code Last} is empty: a quote or housekeeping event (D-2). */
        NON_PRICE,
        /** {@code Last} is set but the trading date or time is empty (D-10, provisionally dropped). */
        MISSING_TIMESTAMP,
        /** Wrong field count, empty symbol, or an unparsable price, date or time. */
        MALFORMED
    }

    private static final int FIELD_COUNT = 7;
    private static final int ID = 0;
    private static final int SEC_TYPE = 1;
    private static final int LAST = 4;
    private static final int TRADING_TIME = 5;
    private static final int TRADING_DATE = 6;

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long SECONDS_PER_DAY = 86_400L;
    private static final long INVALID = Long.MIN_VALUE;

    private final int offsetSeconds;

    private transient String cachedDate;
    private transient long cachedEpochDay;

    /** Interprets trading timestamps as UTC. */
    public TickParser() {
        this(ZoneOffset.UTC);
    }

    public TickParser(ZoneOffset offset) {
        this.offsetSeconds = offset.getTotalSeconds();
    }

    /**
     * Classifies {@code line}. For {@link Outcome#PRICE}, fills {@code symbol}, {@code secType},
     * {@code price} and {@code eventTimeNanos} of {@code into}. Other outcomes leave it unchanged.
     */
    public Outcome parse(String line, Tick into) {
        String[] fields = line.split(",", -1);
        if (fields.length != FIELD_COUNT || fields[ID].isEmpty()) {
            return Outcome.MALFORMED;
        }
        if (fields[LAST].isEmpty()) {
            return Outcome.NON_PRICE;
        }

        double price;
        try {
            price = Double.parseDouble(fields[LAST]);
        } catch (NumberFormatException e) {
            return Outcome.MALFORMED;
        }
        if (!Double.isFinite(price)) {
            return Outcome.MALFORMED;
        }
        if (fields[TRADING_DATE].isEmpty() || fields[TRADING_TIME].isEmpty()) {
            return Outcome.MISSING_TIMESTAMP;
        }

        long epochDay = parseEpochDay(fields[TRADING_DATE]);
        long nanoOfDay = parseNanoOfDay(fields[TRADING_TIME]);
        if (epochDay == INVALID || nanoOfDay == INVALID) {
            return Outcome.MALFORMED;
        }

        into.symbol = fields[ID];
        into.secType = fields[SEC_TYPE];
        into.price = price;
        into.eventTimeNanos =
                (epochDay * SECONDS_PER_DAY - offsetSeconds) * NANOS_PER_SECOND + nanoOfDay;
        return Outcome.PRICE;
    }

    /** {@code DD-MM-YYYY} to days since 1970-01-01, or {@link #INVALID}. */
    private long parseEpochDay(String date) {
        if (date.equals(cachedDate)) {
            return cachedEpochDay;
        }
        if (date.length() != 10 || date.charAt(2) != '-' || date.charAt(5) != '-') {
            return INVALID;
        }
        int day = digits(date, 0, 2);
        int month = digits(date, 3, 5);
        int year = digits(date, 6, 10);
        if (day < 0 || month < 0 || year < 0) {
            return INVALID;
        }
        long epochDay;
        try {
            epochDay = LocalDate.of(year, month, day).toEpochDay();
        } catch (DateTimeException e) {
            return INVALID;
        }
        cachedDate = date;
        cachedEpochDay = epochDay;
        return epochDay;
    }

    /** {@code HH:MM:SS[.fraction]} to nanoseconds since midnight, or {@link #INVALID}. */
    private static long parseNanoOfDay(String time) {
        int length = time.length();
        if (length < 8 || time.charAt(2) != ':' || time.charAt(5) != ':') {
            return INVALID;
        }
        int hours = digits(time, 0, 2);
        int minutes = digits(time, 3, 5);
        int seconds = digits(time, 6, 8);
        if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59) {
            return INVALID;
        }

        long fractionNanos = 0;
        if (length > 8) {
            int fractionDigits = length - 9;
            if (time.charAt(8) != '.' || fractionDigits < 1 || fractionDigits > 9) {
                return INVALID;
            }
            int fraction = digits(time, 9, length);
            if (fraction < 0) {
                return INVALID;
            }
            fractionNanos = fraction;
            for (int i = fractionDigits; i < 9; i++) {
                fractionNanos *= 10;
            }
        }
        return ((hours * 60L + minutes) * 60L + seconds) * NANOS_PER_SECOND + fractionNanos;
    }

    /** Parses {@code s[from, to)} as a non-negative decimal integer, or returns -1. */
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
