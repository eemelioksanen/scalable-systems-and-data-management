package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.nanos;
import static fi.aalto.cse4780.trading.TickParser.Outcome.MALFORMED;
import static fi.aalto.cse4780.trading.TickParser.Outcome.MISSING_TIMESTAMP;
import static fi.aalto.cse4780.trading.TickParser.Outcome.NON_PRICE;
import static fi.aalto.cse4780.trading.TickParser.Outcome.PRICE;
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
                "RDSA.NL,E,08-11-2021,09:00:01.2345,19.2800,09:00:01.1000,08-11-2021", tick));

        assertEquals("RDSA.NL", tick.symbol);
        assertEquals("E", tick.secType);
        assertEquals(19.28, tick.price);
        assertEquals(nanos("2021-11-08T09:00:01.1"), tick.eventTimeNanos);
    }

    @Test
    void usesTheTradingTimestampNotTheSystemTimestamp() {
        assertEquals(PRICE, parser.parse(
                "RDSA.NL,E,09-11-2021,08:00:00.0000,19.28,17:35:00.0000,08-11-2021", tick));

        assertEquals(nanos("2021-11-08T17:35:00"), tick.eventTimeNanos);
    }

    @Test
    void keepsSubMillisecondPrecision() {
        parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.1234,08-11-2021", tick);
        assertEquals(nanos("2021-11-08T09:00:01.1234"), tick.eventTimeNanos);

        parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.123456789,08-11-2021", tick);
        assertEquals(nanos("2021-11-08T09:00:01.123456789"), tick.eventTimeNanos);
    }

    @Test
    void acceptsTimesWithoutFraction() {
        assertEquals(PRICE,
                parser.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01,08-11-2021", tick));
        assertEquals(nanos("2021-11-08T09:00:01"), tick.eventTimeNanos);
    }

    @Test
    void handlesDateChangesBetweenRecords() {
        parser.parse("RDSA.NL,E,08-11-2021,23:59:59,19.28,23:59:59.9000,08-11-2021", tick);
        assertEquals(nanos("2021-11-08T23:59:59.9"), tick.eventTimeNanos);

        parser.parse("RDSA.NL,E,09-11-2021,00:00:01,19.30,00:00:00.1000,09-11-2021", tick);
        assertEquals(nanos("2021-11-09T00:00:00.1"), tick.eventTimeNanos);
    }

    @Test
    void appliesTheZoneOffset() {
        TickParser cet = new TickParser(ZoneOffset.ofHours(1));
        cet.parse("RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.1000,08-11-2021", tick);

        assertEquals(nanos("2021-11-08T08:00:01.1"), tick.eventTimeNanos);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "RDSA.NL,E,08-11-2021,09:00:01.4567,,,",
        "RDSA.NL,E,08-11-2021,09:00:01.4567,,09:00:00.0000,08-11-2021",
        "RDSA.NL,,,,,,"
    })
    void classifiesRowsWithoutLastAsNonPrice(String line) {
        assertEquals(NON_PRICE, parser.parse(line, tick));
        assertNull(tick.symbol);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.1000,",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,,"
    })
    void classifiesPricesWithoutTradingTimestamp(String line) {
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
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,24:00:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:60:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,9:00:00,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.1234567890,08-11-2021",
        "RDSA.NL,E,08-11-2021,09:00:01,19.28,09:00:01.12a4,08-11-2021"
    })
    void rejectsMalformedRecords(String line) {
        assertEquals(MALFORMED, parser.parse(line, tick));
    }
}
