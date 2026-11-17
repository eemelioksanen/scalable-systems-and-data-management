package fi.aalto.cse4780.trading;

import java.io.Serializable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Parses and classifies one {@code ticks} record (docs/interfaces.md):
 *
 * <pre>ID,SecType,Date,Time,Last,Trading time,Trading date</pre>
 *
 * <p>Event time is {@code Trading date} + {@code Trading time}. When {@code Trading date} is empty,
 * as it is on about 92% of price rows, the system {@code Date} is used instead (D-25). Dates are
 * {@code DD-MM-YYYY}. Times are {@code HH:MM:SS[.fraction]}. The source has milliseconds (D-22),
 * and any finer digits are truncated. Timestamps are local times at a fixed {@link ZoneOffset}
 * (D-3). Any whole-hour offset keeps 5-minute windows aligned to local midnight.
 *
 * <p>{@link #parseBinary} reads the binary value of D-42 (docs/interfaces.md) and classifies it
 * by the same rules, in the same order. Its typed fields carry the values this class would parse
 * from the text; anything else arrives as text and is parsed here.
 *
 * <p>Not thread-safe: it caches the last parsed date and reuses its binary fields. Use one
 * instance per source reader.
 */
public final class TickParser implements Serializable {

    private static final long serialVersionUID = 2L;

    /** How a record was classified. Only price outcomes continue into the query. */
    public enum Outcome {
        /** A positive {@code Last} with a complete trading timestamp. */
        PRICE,
        /** A positive {@code Last} whose empty {@code Trading date} was replaced by {@code Date} (D-25). */
        PRICE_SYSTEM_DATE,
        /** {@code Last} is empty: a quote or housekeeping event (D-2). */
        NON_PRICE,
        /** {@code Last} is zero or negative, so not a trade (D-2). */
        NON_POSITIVE_PRICE,
        /** A positive {@code Last} without a {@code Trading time}, or with neither date field (D-10). */
        MISSING_TIMESTAMP,
        /** Wrong field count, empty symbol, or an unparsable price, date or time. */
        MALFORMED;

        public boolean isPrice() {
            return this == PRICE || this == PRICE_SYSTEM_DATE;
        }
    }

    private static final int FIELD_COUNT = 7;
    private static final int ID = 0;
    private static final int SEC_TYPE = 1;
    private static final int DATE = 2;
    private static final int LAST = 4;
    private static final int TRADING_TIME = 5;
    private static final int TRADING_DATE = 6;

    private static final long MILLIS_PER_SECOND = 1_000L;
    private static final long SECONDS_PER_DAY = 86_400L;
    private static final long INVALID = Long.MIN_VALUE;

    static final byte BINARY_VERSION = 1;
    static final byte EMPTY = 0;
    static final byte TEXT = 1;
    static final byte DAY = 2;
    static final byte TIME = 3;
    static final byte NUMBER = 4;

    private final int offsetSeconds;

    private transient String cachedDate;
    private transient long cachedEpochDay;
    private transient BinaryRow binary;

    /** Interprets trading timestamps as UTC. */
    public TickParser() {
        this(ZoneOffset.UTC);
    }

    public TickParser(ZoneOffset offset) {
        this.offsetSeconds = offset.getTotalSeconds();
    }

    /**
     * Classifies {@code line}. For a price outcome, fills {@code symbol}, {@code secType},
     * {@code price} and {@code eventTimeMillis} of {@code into}. Other outcomes leave it unchanged.
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
        if (price <= 0) {
            return Outcome.NON_POSITIVE_PRICE;
        }

        boolean systemDate = fields[TRADING_DATE].isEmpty();
        String date = systemDate ? fields[DATE] : fields[TRADING_DATE];
        if (fields[TRADING_TIME].isEmpty() || date.isEmpty()) {
            return Outcome.MISSING_TIMESTAMP;
        }

        long epochDay = parseEpochDay(date);
        long millisOfDay = parseMillisOfDay(fields[TRADING_TIME]);
        if (epochDay == INVALID || millisOfDay == INVALID) {
            return Outcome.MALFORMED;
        }

        into.symbol = fields[ID];
        into.secType = fields[SEC_TYPE];
        into.price = price;
        into.eventTimeMillis =
                (epochDay * SECONDS_PER_DAY - offsetSeconds) * MILLIS_PER_SECOND + millisOfDay;
        return systemDate ? Outcome.PRICE_SYSTEM_DATE : Outcome.PRICE;
    }

    /**
     * Classifies a binary {@code ticks} value as {@link #parse} classifies the CSV one. A value
     * that does not follow the layout is {@link Outcome#MALFORMED}.
     */
    public Outcome parseBinary(byte[] value, Tick into) {
        if (binary == null) {
            binary = new BinaryRow();
        }
        BinaryRow row = binary;
        ByteBuffer in = ByteBuffer.wrap(value);
        try {
            if (in.get() != BINARY_VERSION) {
                return Outcome.MALFORMED;
            }
            row.id.read(in);
            row.secType.read(in);
            row.date.read(in);
            row.time.read(in);
            row.last.read(in);
            row.tradingTime.read(in);
            row.tradingDate.read(in);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            return Outcome.MALFORMED;
        }
        if (in.hasRemaining() || row.id.tag != TEXT || !row.secType.isText()) {
            return Outcome.MALFORMED;
        }

        double price;
        switch (row.last.tag) {
            case EMPTY:
                return Outcome.NON_PRICE;
            case NUMBER:
                price = row.last.number;
                break;
            case TEXT:
                try {
                    price = Double.parseDouble(row.last.text);
                } catch (NumberFormatException e) {
                    return Outcome.MALFORMED;
                }
                break;
            default:
                return Outcome.MALFORMED;
        }
        if (!Double.isFinite(price)) {
            return Outcome.MALFORMED;
        }
        if (price <= 0) {
            return Outcome.NON_POSITIVE_PRICE;
        }

        boolean systemDate = row.tradingDate.tag == EMPTY;
        BinaryField date = systemDate ? row.date : row.tradingDate;
        if (row.tradingTime.tag == EMPTY || date.tag == EMPTY) {
            return Outcome.MISSING_TIMESTAMP;
        }

        long epochDay = switch (date.tag) {
            case DAY -> date.integer;
            case TEXT -> parseEpochDay(date.text);
            default -> INVALID;
        };
        long millisOfDay = switch (row.tradingTime.tag) {
            case TIME -> row.tradingTime.integer;
            case TEXT -> parseMillisOfDay(row.tradingTime.text);
            default -> INVALID;
        };
        if (epochDay == INVALID || millisOfDay == INVALID) {
            return Outcome.MALFORMED;
        }

        into.symbol = row.id.text;
        into.secType = row.secType.tag == TEXT ? row.secType.text : "";
        into.price = price;
        into.eventTimeMillis =
                (epochDay * SECONDS_PER_DAY - offsetSeconds) * MILLIS_PER_SECOND + millisOfDay;
        return systemDate ? Outcome.PRICE_SYSTEM_DATE : Outcome.PRICE;
    }

    /** The seven fields of a binary value, in projection order, reused from value to value. */
    private static final class BinaryRow {
        final BinaryField id = new BinaryField();
        final BinaryField secType = new BinaryField();
        final BinaryField date = new BinaryField();
        final BinaryField time = new BinaryField();
        final BinaryField last = new BinaryField();
        final BinaryField tradingTime = new BinaryField();
        final BinaryField tradingDate = new BinaryField();
    }

    /** One binary field: a tag byte, then nothing, text, an int or a double. */
    private static final class BinaryField {
        byte tag;
        String text;
        int integer;
        double number;

        void read(ByteBuffer in) {
            tag = in.get();
            switch (tag) {
                case EMPTY -> {}
                case TEXT -> {
                    int length = Short.toUnsignedInt(in.getShort());
                    if (length > in.remaining()) {
                        throw new IllegalArgumentException("text runs past the value");
                    }
                    text = new String(in.array(), in.position(), length, StandardCharsets.UTF_8);
                    in.position(in.position() + length);
                }
                case DAY, TIME -> integer = in.getInt();
                case NUMBER -> number = in.getDouble();
                default -> throw new IllegalArgumentException("unknown tag " + tag);
            }
        }

        boolean isText() {
            return tag == EMPTY || tag == TEXT;
        }
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

    /**
     * {@code HH:MM:SS[.fraction]} with 1-9 fraction digits to milliseconds since midnight, or
     * {@link #INVALID}. Digits beyond milliseconds are truncated.
     */
    private static long parseMillisOfDay(String time) {
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

        long millis = 0;
        if (length > 8) {
            int fractionDigits = length - 9;
            if (time.charAt(8) != '.' || fractionDigits < 1 || fractionDigits > 9) {
                return INVALID;
            }
            int fraction = digits(time, 9, length);
            if (fraction < 0) {
                return INVALID;
            }
            millis = fraction;
            for (int i = fractionDigits; i < 3; i++) {
                millis *= 10;
            }
            for (int i = 3; i < fractionDigits; i++) {
                millis /= 10;
            }
        }
        return ((hours * 60L + minutes) * 60L + seconds) * MILLIS_PER_SECOND + millis;
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
