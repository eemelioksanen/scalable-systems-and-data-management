package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static fi.aalto.cse4780.trading.TickParser.Outcome.MALFORMED;
import static fi.aalto.cse4780.trading.TickParser.Outcome.MISSING_TIMESTAMP;
import static fi.aalto.cse4780.trading.TickParser.Outcome.NON_POSITIVE_PRICE;
import static fi.aalto.cse4780.trading.TickParser.Outcome.NON_PRICE;
import static fi.aalto.cse4780.trading.TickParser.Outcome.PRICE;
import static fi.aalto.cse4780.trading.TickParser.Outcome.PRICE_SYSTEM_DATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TickParserTest {

    private final TickParser parser = new TickParser();
    private final Tick tick = new Tick();

    @Test
    void parsesPriceEvents() {
        assertEquals(PRICE, parser.parse(
                "RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.100,08-11-2021", tick));

        assertEquals("RDSA.NL", tick.symbol);
        assertEquals("E", tick.secType);
        assertEquals(19.28, tick.price);
        assertEquals(millis("2021-11-08T09:00:01.100"), tick.eventTimeMillis);
    }

    @Test
    void keepsNumericSymbolsAsStrings() {
        assertEquals(PRICE, parser.parse(
                "846741.ETR,E,08-11-2021,09:00:01.123,12.500000,09:00:01.100,08-11-2021", tick));
        assertEquals("846741.ETR", tick.symbol);
    }

    @Test
    void usesTheTradingDateAndTimeWhenBothArePresent() {
        assertEquals(PRICE, parser.parse(
                "RDSA.NL,E,09-11-2021,08:00:00.000,19.28,17:35:00.000,08-11-2021", tick));

        assertEquals(millis("2021-11-08T17:35:00"), tick.eventTimeMillis);
    }

    @Test
    void usesTheSystemDateWhenTradingDateIsEmpty() {
        assertEquals(PRICE_SYSTEM_DATE, parser.parse(
                "RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.100,", tick));

        assertEquals(millis("2021-11-08T09:00:01.100"), tick.eventTimeMillis);
        assertEquals(19.28, tick.price);
    }

    @Test
    void truncatesDigitsBeyondMilliseconds() {
        parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.123456789,08-11-2021", tick);
        assertEquals(millis("2021-11-08T09:00:01.123"), tick.eventTimeMillis);

        parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.5,08-11-2021", tick);
        assertEquals(millis("2021-11-08T09:00:01.500"), tick.eventTimeMillis);
    }

    @Test
    void acceptsTimesWithoutFraction() {
        assertEquals(PRICE,
                parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01,08-11-2021", tick));
        assertEquals(millis("2021-11-08T09:00:01"), tick.eventTimeMillis);
    }

    @Test
    void handlesDateChangesBetweenRecords() {
        parser.parse("RDSA.NL,E,08-11-2021,23:59:59.950,19.28,23:59:59.900,", tick);
        assertEquals(millis("2021-11-08T23:59:59.900"), tick.eventTimeMillis);

        parser.parse("RDSA.NL,E,09-11-2021,00:00:01.000,19.30,00:00:00.100,", tick);
        assertEquals(millis("2021-11-09T00:00:00.100"), tick.eventTimeMillis);
    }

    @Test
    void appliesTheZoneOffset() {
        TickParser cet = new TickParser(ZoneOffset.ofHours(1));
        cet.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.100,08-11-2021", tick);

        assertEquals(millis("2021-11-08T08:00:01.100"), tick.eventTimeMillis);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "RDSA.NL,E,08-11-2021,09:00:01.456,,,",
        "RDSA.NL,E,08-11-2021,09:00:01.456,,09:00:00.000,08-11-2021",
        "RDSA.NL,,,,,,"
    })
    void classifiesRowsWithoutLastAsNonPrice(String line) {
        assertEquals(NON_PRICE, parser.parse(line, tick));
        assertNull(tick.symbol);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "RDSA.NL,E,08-11-2021,09:00:01.456,0.000000,09:00:01.100,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01.456,-1.5,09:00:01.100,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01.456,0.000000,,"
    })
    void classifiesZeroAndNegativePricesAsNonPositive(String line) {
        assertEquals(NON_POSITIVE_PRICE, parser.parse(line, tick));
        assertNull(tick.symbol);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,,",
        "RDSA.NL,E,,09:00:01,19.28,09:00:01.100,"
    })
    void classifiesPricesWithoutUsableTimestamp(String line) {
        assertEquals(MISSING_TIMESTAMP, parser.parse(line, tick));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "RDSA.NL,E",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01,08-11-2021,extra",
        ",E,08-11-2021,09:00:01,19.28,09:00:01,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,abc,09:00:01,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,NaN,09:00:01,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,Infinity,09:00:01,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01,2021-11-08",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01,31-02-2021",
        "RDSA.NL,E,2021-11-08,09:00:01,19.28,09:00:01,",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,24:00:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:60:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,9:00:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.1234567890,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.12a,08-11-2021"
    })
    void rejectsMalformedRecords(String line) {
        assertEquals(MALFORMED, parser.parse(line, tick));
    }
}
