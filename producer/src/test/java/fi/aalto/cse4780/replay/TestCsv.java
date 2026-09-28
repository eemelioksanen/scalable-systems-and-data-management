package fi.aalto.cse4780.replay;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Synthetic day files with the real files' structure (docs/data-contract.md §2). */
final class TestCsv {

    /** The real header row: 39 names, some with a leading space, no trailing delimiter. */
    static final String HEADER = "ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,"
            + "Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,"
            + "Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume,Mid price,"
            + "Trading date,Profit,Current price,Related indices,Day high bid time, Day low bid time,Open Time,"
            + " Last trade time, Close Time, Day high Time, Day low Time,Bid time,Auction Time";

    /** Eleven CRLF comment lines, the header, and a comment line after it: data starts on line 14. */
    static final String PRELUDE = "# DEBS 2022 Grand Challenge Data Set Trading Data (synthetic test copy)\r\n"
            + "#\r\n".repeat(10)
            + HEADER + "\n"
            + "#'Unique ID','Security Type (E)quity/(I)ndex', ...\n";

    static final int FIRST_DATA_LINE = 14;

    private TestCsv() {}

    /**
     * A data row: the 7 projected fields in place, an {@code Ask} to show that other columns are
     * dropped, and the trailing delimiter that gives 40 fields.
     */
    static String row(String id, String date, String time, String last, String tradingTime, String tradingDate) {
        String[] fields = new String[CsvLine.DECLARED_FIELDS + 1];
        Arrays.fill(fields, "");
        fields[CsvLine.ID] = id;
        fields[CsvLine.SEC_TYPE] = id.startsWith("I") ? "I" : "E";
        fields[CsvLine.DATE] = date;
        fields[CsvLine.TIME] = time;
        fields[4] = "99.990000";
        fields[CsvLine.LAST] = last;
        fields[CsvLine.TRADING_TIME] = tradingTime;
        fields[CsvLine.TRADING_DATE] = tradingDate;
        return String.join(",", fields);
    }

    /** A price row on 08.11.2021 whose system and trading times are both {@code time}. */
    static String price(String id, String time, String last) {
        return row(id, "08-11-2021", time, last, time, "");
    }

    /** A quote or housekeeping row: no {@code Last}. */
    static String quote(String id, String time) {
        return row(id, "08-11-2021", time, "", "", "");
    }

    /**
     * Two day files in {@code dir}, 09.11 created first: 08.11 holds a malformed row on line 16 and
     * rows of all three exchanges, 09.11 two more rows.
     */
    static Path week(Path dir) throws IOException {
        write(dir, "debs2022-gc-trading-day-09-11-21.csv",
                price("RDSA.NL", "09:00:00.000", "19.300000").replace("08-11-2021", "09-11-2021"),
                quote("SIE.ETR", "09:00:00.500").replace("08-11-2021", "09-11-2021"));
        write(dir, "debs2022-gc-trading-day-08-11-21.csv",
                price("RDSA.NL", "09:00:01.123", "19.280000"),
                quote("AIR.FR", "09:00:01.200"),
                "AIR.FR,E,08-11-2021,09:00:01.300,only-four-fields",
                price("AIR.FR", "09:00:02.000", "111.940000"),
                price("846741.ETR", "09:00:02.500", "12.500000"));
        return dir;
    }

    static Path write(Path dir, String name, String... dataRows) throws IOException {
        StringBuilder text = new StringBuilder(PRELUDE);
        for (String row : dataRows) {
            text.append(row).append('\n');
        }
        return Files.writeString(dir.resolve(name), text, StandardCharsets.UTF_8);
    }
}
