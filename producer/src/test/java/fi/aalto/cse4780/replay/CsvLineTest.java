package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.CsvLine.Kind.COMMENT;
import static fi.aalto.cse4780.replay.CsvLine.Kind.DATA;
import static fi.aalto.cse4780.replay.CsvLine.Kind.HEADER;
import static fi.aalto.cse4780.replay.CsvLine.Kind.REJECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CsvLineTest {

    private final CsvLine line = new CsvLine();

    @Test
    void projectsTheSevenTicksFieldsVerbatim() {
        String row = TestCsv.row("RDSA.NL", "08-11-2021", "09:00:01.123", "19.280000", "09:00:01.100", "08-11-2021");

        assertEquals(DATA, line.parse(row));
        assertEquals("RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.100,08-11-2021", line.projection());
        assertEquals("RDSA.NL", line.field(CsvLine.ID));
    }

    @Test
    void keepsEmptyFieldsAsEmpty() {
        assertEquals(DATA, line.parse(TestCsv.quote("I2HCA.FR", "")));
        assertEquals("I2HCA.FR,I,08-11-2021,,,,", line.projection());

        assertEquals(DATA, line.parse(TestCsv.row("X.FR", "", "", "0.000000", "00:00:00.000", "")));
        assertEquals("X.FR,E,,,0.000000,00:00:00.000,", line.projection());
    }

    @Test
    void acceptsRowsWithAndWithoutTheTrailingDelimiter() {
        String with = TestCsv.price("AIR.FR", "09:00:00.000", "111.940000");
        String without = with.substring(0, with.length() - 1);

        assertEquals(DATA, line.parse(with));
        String projected = line.projection();
        assertEquals(DATA, line.parse(without));
        assertEquals(projected, line.projection());
    }

    @Test
    void rejectsRowsThatCannotBeProjected() {
        String row = TestCsv.price("AIR.FR", "09:00:00.000", "111.940000");

        assertEquals(REJECTED, line.parse(row + "x"), "a non-empty 40th field");
        assertEquals(REJECTED, line.parse(row + ","), "41 fields");
        assertEquals(REJECTED, line.parse(row.substring(0, row.lastIndexOf(',', row.length() - 2))), "38 fields");
        assertEquals(REJECTED, line.parse(""));
        assertEquals(REJECTED, line.parse(",,,,,,,,,,,,,,,"));
    }

    @Test
    void recognisesCommentsAndTheHeader() {
        assertEquals(COMMENT, line.parse("# DEBS 2022 Grand Challenge"));
        assertEquals(COMMENT, line.parse("#'Unique ID','Security Type (E)quity/(I)ndex'"));
        assertEquals(HEADER, line.parse(TestCsv.HEADER));
    }
}
