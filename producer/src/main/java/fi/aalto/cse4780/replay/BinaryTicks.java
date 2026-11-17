package fi.aalto.cse4780.replay;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Arrays;

/**
 * The binary {@code ticks} value (D-42, docs/interfaces.md): a version byte, then the 7 projected
 * fields in CSV order, each a tag byte and its payload.
 *
 * <p>Dates, times and the price become numbers only when the job's parser would read the same
 * number from the text. Any other non-empty field stays text, which the job then parses itself,
 * so every row is classified exactly as its CSV value would be.
 *
 * <p>Not thread-safe: it reuses one buffer.
 */
final class BinaryTicks {

    static final byte VERSION = 1;
    static final byte EMPTY = 0;
    static final byte TEXT = 1;
    static final byte DAY = 2;
    static final byte TIME = 3;
    static final byte NUMBER = 4;

    static final int MAX_TEXT_BYTES = 0xFFFF;

    private final ByteBuffer buffer = ByteBuffer.allocate(1 + 7 * (3 + MAX_TEXT_BYTES));

    /** The value of a {@link CsvLine.Kind#DATA} row. Throws if a field exceeds {@link #MAX_TEXT_BYTES}. */
    byte[] encode(CsvLine row) {
        buffer.clear();
        buffer.put(VERSION);
        text(row.field(CsvLine.ID));
        text(row.field(CsvLine.SEC_TYPE));
        date(row.field(CsvLine.DATE));
        time(row.field(CsvLine.TIME));
        number(row.field(CsvLine.LAST));
        time(row.field(CsvLine.TRADING_TIME));
        date(row.field(CsvLine.TRADING_DATE));
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    private void text(String field) {
        if (field.isEmpty()) {
            buffer.put(EMPTY);
            return;
        }
        byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "a field of " + bytes.length + " bytes is longer than the binary encoding allows");
        }
        buffer.put(TEXT).putShort((short) bytes.length).put(bytes);
    }

    /** {@code DD-MM-YYYY} as days since 1970-01-01. */
    private void date(String field) {
        boolean shape = field.length() == 10 && field.charAt(2) == '-' && field.charAt(5) == '-'
                && digits(field, 0, 2) >= 0 && digits(field, 3, 5) >= 0 && digits(field, 6, 10) >= 0;
        if (!shape) {
            text(field);
            return;
        }
        long epochDay;
        try {
            epochDay = LocalDate.of(digits(field, 6, 10), digits(field, 3, 5), digits(field, 0, 2)).toEpochDay();
        } catch (DateTimeException invalid) {
            text(field);
            return;
        }
        buffer.put(DAY).putInt((int) epochDay);
    }

    /** {@code HH:MM:SS} or {@code HH:MM:SS.mmm} as milliseconds since midnight. */
    private void time(String field) {
        int length = field.length();
        boolean shape = (length == 8 || (length == 12 && field.charAt(8) == '.'))
                && field.charAt(2) == ':' && field.charAt(5) == ':';
        if (shape) {
            int hours = digits(field, 0, 2);
            int minutes = digits(field, 3, 5);
            int seconds = digits(field, 6, 8);
            int millis = length == 12 ? digits(field, 9, 12) : 0;
            if (hours >= 0 && hours <= 23 && minutes >= 0 && minutes <= 59 && seconds >= 0 && seconds <= 59
                    && millis >= 0) {
                buffer.put(TIME).putInt(((hours * 60 + minutes) * 60 + seconds) * 1000 + millis);
                return;
            }
        }
        text(field);
    }

    private void number(String field) {
        double value;
        try {
            value = Double.parseDouble(field);
        } catch (NumberFormatException notANumber) {
            text(field);
            return;
        }
        buffer.put(NUMBER).putDouble(value);
    }

    /** {@code s[from, to)} as a non-negative decimal integer, or -1. */
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
