package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Operator tests for Query 1. Expected EMAs were computed independently with exact rational
 * arithmetic (alpha = 2 / (1 + j), EMA0 = 0) and rounded to double.
 */
class EmaCalculatorTest {

    private static final double TOLERANCE = 1e-12;
    private static final String SYMBOL = "RDSA.NL";

    private KeyedOneInputStreamOperatorTestHarness<String, Tick, EmaResult> harness;
    private long nextSourceOrder;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new EmaCalculator(), tick -> tick.symbol, Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void evaluatesConsecutiveWindowsFromZeroInitialEmas() throws Exception {
        send(SYMBOL, "2021-11-08T09:01:00", 99.0);
        send(SYMBOL, "2021-11-08T09:04:00", 100.0);
        send(SYMBOL, "2021-11-08T09:06:00", 101.0);
        send(SYMBOL, "2021-11-08T09:12:30", 99.5);
        watermark("2021-11-08T09:15:00");

        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(3, results.size());
        assertResult(results.get(0), SYMBOL, "2021-11-08T09:05:00", 100.0,
                5.128205128205129, 1.9801980198019802);
        assertResult(results.get(1), SYMBOL, "2021-11-08T09:10:00", 101.0,
                10.044707429322814, 3.940986177825703);
        assertResult(results.get(2), SYMBOL, "2021-11-08T09:15:00", 99.5,
                14.632158330383183, 5.8332438772746995);
    }

    @Test
    void evaluatesAWindowOnlyOnceTheNextWindowHasStarted() throws Exception {
        send(SYMBOL, "2021-11-08T09:01:00", 100.0);

        harness.processWatermark(millis("2021-11-08T09:05:00") - 2);
        assertEquals(0, harness.extractOutputValues().size());

        harness.processWatermark(millis("2021-11-08T09:05:00") - 1);
        assertEquals(1, harness.extractOutputValues().size());
    }

    @Test
    void closeIsTheLatestEventTimeNotTheLastArrival() throws Exception {
        send(SYMBOL, "2021-11-08T09:04:00", 10.0);
        send(SYMBOL, "2021-11-08T09:02:00", 20.0);
        watermark("2021-11-08T09:05:00");

        assertEquals(10.0, single().close);
    }

    @Test
    void equalEventTimesGoToTheLaterArrival() throws Exception {
        long eventTime = nanos("2021-11-08T09:03:00");
        send(new Tick(SYMBOL, "E", 20.0, eventTime, 7, 0));
        send(new Tick(SYMBOL, "E", 10.0, eventTime, 5, 0));
        watermark("2021-11-08T09:05:00");

        assertEquals(20.0, single().close);
    }

    @Test
    void subMillisecondPrecisionSelectsTheLatestTrade() throws Exception {
        long eventTime = nanos("2021-11-08T09:03:00.1234");
        send(new Tick(SYMBOL, "E", 10.0, eventTime + 100_000, 1, 0));
        send(new Tick(SYMBOL, "E", 20.0, eventTime, 2, 0));
        watermark("2021-11-08T09:05:00");

        assertEquals(10.0, single().close);
    }

    @Test
    void windowsWithoutPriceEventsLeaveEmasUnchanged() throws Exception {
        send(SYMBOL, "2021-11-08T09:02:00", 100.0);
        send(SYMBOL, "2021-11-08T09:12:00", 110.0);
        watermark("2021-11-08T09:20:00");

        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(2, results.size());
        assertResult(results.get(0), SYMBOL, "2021-11-08T09:05:00", 100.0,
                5.128205128205129, 1.9801980198019802);
        assertResult(results.get(1), SYMBOL, "2021-11-08T09:15:00", 110.0,
                10.506245890861276, 4.1192039996078815);
    }

    @Test
    void keepsEveryOpenWindowWhenEventsArriveOutOfOrder() throws Exception {
        send(SYMBOL, "2021-11-08T09:06:00", 50.0);
        send(SYMBOL, "2021-11-08T09:03:00", 40.0);
        send(SYMBOL, "2021-11-08T09:08:00", 55.0);
        send(SYMBOL, "2021-11-08T09:01:00", 41.0);
        watermark("2021-11-08T09:10:00");

        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(2, results.size());
        assertResult(results.get(0), SYMBOL, "2021-11-08T09:05:00", 40.0,
                2.051282051282051, 0.7920792079207921);
        assertResult(results.get(1), SYMBOL, "2021-11-08T09:10:00", 55.0,
                4.7666009204470745, 1.8655033820213704);
    }

    @Test
    void lateEventsGoToTheSideOutputWithoutChangingResults() throws Exception {
        send(SYMBOL, "2021-11-08T09:01:00", 100.0);
        watermark("2021-11-08T09:05:00");
        send(SYMBOL, "2021-11-08T09:02:00", 200.0);
        watermark("2021-11-08T09:10:00");

        assertEquals(100.0, single().close);
        List<Tick> late = harness.getSideOutput(EmaCalculator.LATE_TICKS).stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toList());
        assertEquals(1, late.size());
        assertEquals(200.0, late.get(0).price);
    }

    @Test
    void acceptsEventsBehindTheWatermarkWhileTheirWindowIsOpen() throws Exception {
        watermark("2021-11-08T09:07:00");
        send(SYMBOL, "2021-11-08T09:06:00", 42.0);
        watermark("2021-11-08T09:10:00");

        assertResult(single(), SYMBOL, "2021-11-08T09:10:00", 42.0,
                42.0 * 2 / 39, 42.0 * 2 / 101);
        assertTrue(harness.getSideOutput(EmaCalculator.LATE_TICKS) == null
                || harness.getSideOutput(EmaCalculator.LATE_TICKS).isEmpty());
    }

    @Test
    void windowsIncludeTheirStartAndExcludeTheirEnd() throws Exception {
        send(SYMBOL, "2021-11-08T09:05:00", 1.0);
        send(SYMBOL, "2021-11-08T09:04:59.999", 2.0);
        watermark("2021-11-08T09:10:00");

        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(2, results.size());
        assertEquals(millis("2021-11-08T09:05:00"), results.get(0).windowEnd);
        assertEquals(2.0, results.get(0).close);
        assertEquals(millis("2021-11-08T09:10:00"), results.get(1).windowEnd);
        assertEquals(1.0, results.get(1).close);
    }

    @Test
    void emaStateCarriesOverMidnight() throws Exception {
        send(SYMBOL, "2021-11-08T23:58:00", 10.0);
        send(SYMBOL, "2021-11-09T00:01:00", 12.0);
        watermark("2021-11-09T00:05:00");

        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(2, results.size());
        assertResult(results.get(0), SYMBOL, "2021-11-09T00:00:00", 10.0,
                0.5128205128205128, 0.19801980198019803);
        assertResult(results.get(1), SYMBOL, "2021-11-09T00:05:00", 12.0,
                1.1019066403681788, 0.431722380158808);
    }

    @Test
    void symbolsAreCalculatedIndependently() throws Exception {
        send("AAA.FR", "2021-11-08T09:01:00", 100.0);
        send("BBB.ETR", "2021-11-08T09:02:00", 40.0);
        watermark("2021-11-08T09:05:00");

        List<EmaResult> results = harness.extractOutputValues().stream()
                .sorted(Comparator.comparing(r -> r.symbol))
                .collect(Collectors.toList());
        assertEquals(2, results.size());
        assertResult(results.get(0), "AAA.FR", "2021-11-08T09:05:00", 100.0,
                5.128205128205129, 1.9801980198019802);
        assertResult(results.get(1), "BBB.ETR", "2021-11-08T09:05:00", 40.0,
                2.051282051282051, 0.7920792079207921);
    }

    @Test
    void resultCarriesSecTypeAndLatestIngestTimeOfTheWindow() throws Exception {
        send(new Tick("CAC.FR", "I", 7000.0, nanos("2021-11-08T09:04:00"), 1, 1_000));
        send(new Tick("CAC.FR", "I", 7001.0, nanos("2021-11-08T09:02:00"), 2, 3_000));
        long before = System.currentTimeMillis();
        watermark("2021-11-08T09:05:00");

        EmaResult result = single();
        assertEquals("I", result.secType);
        assertEquals(7000.0, result.close);
        assertEquals(3_000, result.maxIngestTs);
        assertTrue(result.emitTs >= before);
    }

    @Test
    void evaluatedWindowsReleaseTheirTimers() throws Exception {
        send(SYMBOL, "2021-11-08T09:01:00", 100.0);
        send(SYMBOL, "2021-11-08T09:06:00", 101.0);
        assertEquals(2, harness.numEventTimeTimers());

        watermark("2021-11-08T09:10:00");
        assertEquals(0, harness.numEventTimeTimers());
    }

    private void send(String symbol, String localDateTime, double price) throws Exception {
        send(new Tick(symbol, "E", price, nanos(localDateTime), nextSourceOrder++, 0));
    }

    private void send(Tick tick) throws Exception {
        harness.processElement(tick, tick.eventTimeMillis());
    }

    private void watermark(String localDateTime) throws Exception {
        harness.processWatermark(millis(localDateTime));
    }

    private EmaResult single() {
        List<EmaResult> results = harness.extractOutputValues();
        assertEquals(1, results.size(), () -> "results: " + results);
        return results.get(0);
    }

    private static void assertResult(
            EmaResult actual,
            String symbol,
            String windowEnd,
            double close,
            double ema38,
            double ema100) {
        assertEquals(symbol, actual.symbol);
        assertEquals(millis(windowEnd), actual.windowEnd);
        assertEquals(close, actual.close);
        assertEquals(ema38, actual.ema38, TOLERANCE);
        assertEquals(ema100, actual.ema100, TOLERANCE);
    }

    static long millis(String localDateTime) {
        return LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    static long nanos(String localDateTime) {
        var instant = LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
}
