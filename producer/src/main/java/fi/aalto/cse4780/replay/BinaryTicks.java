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
 * <p>Not thread-safe: it reuses one buffer and caches the last date.
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

    private String cachedDate;
    private int cachedEpochDay;

    /** The value of a {@link CsvLine.Kind#DATA} row. Throws if a field exceeds {@link #MAX_TEXT_BYTES}. */
    byte[] encode(CsvLine row) {
        String line = row.line();
        buffer.clear();
        buffer.put(VERSION);
        text(line, row.start(CsvLine.ID), row.end(CsvLine.ID));
        text(line, row.start(CsvLine.SEC_TYPE), row.end(CsvLine.SEC_TYPE));
        date(line, row.start(CsvLine.DATE), row.end(CsvLine.DATE));
        time(line, row.start(CsvLine.TIME), row.end(CsvLine.TIME));
        number(line, row.start(CsvLine.LAST), row.end(CsvLine.LAST));
        time(line, row.start(CsvLine.TRADING_TIME), row.end(CsvLine.TRADING_TIME));
        date(line, row.start(CsvLine.TRADING_DATE), row.end(CsvLine.TRADING_DATE));
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    /** {@code line[from, to)} as UTF-8 text, or empty. */
    private void text(String line, int from, int to) {
        if (from == to) {
            buffer.put(EMPTY);
            return;
        }
        boolean ascii = true;
        for (int i = from; i < to && ascii; i++) {
            ascii = line.charAt(i) < 0x80;
        }
        byte[] bytes = ascii ? null : line.substring(from, to).getBytes(StandardCharsets.UTF_8);
        int length = ascii ? to - from : bytes.length;
        if (length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "a field of " + length + " bytes is longer than the binary encoding allows");
        }
        buffer.put(TEXT).putShort((short) length);
        if (ascii) {
            for (int i = from; i < to; i++) {
                buffer.put((byte) line.charAt(i));
            }
        } else {
            buffer.put(bytes);
        }
    }

    /** {@code DD-MM-YYYY} as days since 1970-01-01. */
    private void date(String line, int from, int to) {
        if (to - from == 10 && cachedDate != null && line.regionMatches(from, cachedDate, 0, 10)) {
            buffer.put(DAY).putInt(cachedEpochDay);
            return;
        }
        int day = digits(line, from, from + 2);
        int month = digits(line, from + 3, from + 5);
        int year = digits(line, from + 6, from + 10);
        boolean shape = to - from == 10 && line.charAt(from + 2) == '-' && line.charAt(from + 5) == '-'
                && day >= 0 && month >= 0 && year >= 0;
        if (!shape) {
            text(line, from, to);
            return;
        }
        try {
            cachedEpochDay = (int) LocalDate.of(year, month, day).toEpochDay();
        } catch (DateTimeException invalid) {
            text(line, from, to);
            return;
        }
        cachedDate = line.substring(from, to);
        buffer.put(DAY).putInt(cachedEpochDay);
    }

    /** {@code HH:MM:SS} or {@code HH:MM:SS.mmm} as milliseconds since midnight. */
    private void time(String line, int from, int to) {
        int length = to - from;
        boolean shape = (length == 8 || (length == 12 && line.charAt(from + 8) == '.'))
                && line.charAt(from + 2) == ':' && line.charAt(from + 5) == ':';
        if (shape) {
            int hours = digits(line, from, from + 2);
            int minutes = digits(line, from + 3, from + 5);
            int seconds = digits(line, from + 6, from + 8);
            int millis = length == 12 ? digits(line, from + 9, from + 12) : 0;
            if (hours >= 0 && hours <= 23 && minutes >= 0 && minutes <= 59 && seconds >= 0 && seconds <= 59
                    && millis >= 0) {
                buffer.put(TIME).putInt(((hours * 60 + minutes) * 60 + seconds) * 1000 + millis);
                return;
            }
        }
        text(line, from, to);
    }

    private void number(String line, int from, int to) {
        double value;
        try {
            value = Double.parseDouble(line.substring(from, to));
        } catch (NumberFormatException notANumber) {
            text(line, from, to);
            return;
        }
        buffer.put(NUMBER).putDouble(value);
    }

    /** {@code s[from, to)} as a non-negative decimal integer, or -1, also when it runs past {@code s}. */
    private static int digits(String s, int from, int to) {
        if (to > s.length()) {
            return -1;
        }
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
