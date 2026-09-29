package fi.aalto.cse4780.replay;

/**
 * One line of a DEBS 2022 trading-data CSV, split into fields without copying them.
 *
 * <p>Follows the structural tolerances of D-21: lines starting with {@code #} are comments wherever
 * they occur, a data row has the 39 declared fields plus an empty 40th one from the trailing
 * delimiter, there is no quoting, and columns are matched by position.
 *
 * <p>The replayer selects fields but never interprets them (D-13, D-24). Parsing and
 * classification stay in the Flink job.
 *
 * <p>Not thread-safe: it reuses its buffers across lines.
 */
final class CsvLine {

    /** The columns the header declares. */
    static final int DECLARED_FIELDS = 39;

    static final int ID = 0;
    static final int SEC_TYPE = 1;
    static final int DATE = 2;
    static final int TIME = 3;
    static final int LAST = 21;
    static final int TRADING_TIME = 23;
    static final int TRADING_DATE = 26;

    /** The {@code ticks} value, in order: {@code ID,SecType,Date,Time,Last,Trading time,Trading date}. */
    private static final int[] PROJECTION = {ID, SEC_TYPE, DATE, TIME, LAST, TRADING_TIME, TRADING_DATE};

    private static final String HEADER_PREFIX = "ID,SecType,";

    enum Kind {
        COMMENT,
        HEADER,
        DATA,
        /** Neither 39 fields nor 40 with an empty last one, so there is nothing safe to project. */
        REJECTED
    }

    private String line;
    /** Index of the delimiter, or the line end, that closes each field. */
    private final int[] fieldEnd = new int[DECLARED_FIELDS + 1];
    private final StringBuilder projection = new StringBuilder(96);

    Kind parse(String line) {
        this.line = line;
        if (line.startsWith("#")) {
            return Kind.COMMENT;
        }
        if (line.startsWith(HEADER_PREFIX)) {
            return Kind.HEADER;
        }
        int fields = 0;
        int from = 0;
        while (true) {
            if (fields == fieldEnd.length) {
                return Kind.REJECTED;
            }
            int comma = line.indexOf(',', from);
            fieldEnd[fields++] = comma < 0 ? line.length() : comma;
            if (comma < 0) {
                break;
            }
            from = comma + 1;
        }
        boolean trailingEmpty = fields == DECLARED_FIELDS + 1 && isEmpty(DECLARED_FIELDS);
        return fields == DECLARED_FIELDS || trailingEmpty ? Kind.DATA : Kind.REJECTED;
    }

    /** The line last passed to {@link #parse}. */
    String line() {
        return line;
    }

    int start(int field) {
        return field == 0 ? 0 : fieldEnd[field - 1] + 1;
    }

    int end(int field) {
        return fieldEnd[field];
    }

    boolean isEmpty(int field) {
        return start(field) == end(field);
    }

    String field(int field) {
        return line.substring(start(field), end(field));
    }

    /** The 7-field {@code ticks} value of a {@link Kind#DATA} line (D-24), with the source text unchanged. */
    String projection() {
        projection.setLength(0);
        for (int i = 0; i < PROJECTION.length; i++) {
            if (i > 0) {
                projection.append(',');
            }
            projection.append(line, start(PROJECTION[i]), end(PROJECTION[i]));
        }
        return projection.toString();
    }
}
