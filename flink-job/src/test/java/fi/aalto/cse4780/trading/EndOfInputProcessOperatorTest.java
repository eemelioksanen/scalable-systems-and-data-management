package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class EndOfInputProcessOperatorTest {

    @Test
    void endOfInputEvaluatesTheOpenWindowsInOrderWithoutAnyWatermark() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, Tick, EmaResult> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new EndOfInputProcessOperator<>(new EmaCalculator(Variant.EmptyWindows.SKIP)),
                        tick -> tick.symbol,
                        Types.STRING)) {
            harness.open();
            Tick later = new Tick("RDSA.NL", "E", 101.0, millis("2021-11-08T09:06:00"), 0, 0);
            Tick earlier = new Tick("RDSA.NL", "E", 100.0, millis("2021-11-08T09:01:00"), 1, 0);
            harness.processElement(later, later.eventTimeMillis);
            harness.processElement(earlier, earlier.eventTimeMillis);
            assertEquals(0, harness.extractOutputValues().size());

            harness.endInput();

            List<EmaResult> results = harness.extractOutputValues();
            assertEquals(2, results.size());
            assertEquals(millis("2021-11-08T09:05:00"), results.get(0).windowEnd);
            assertEquals(5.128205128205129, results.get(0).ema38, 1e-12);
            assertEquals(millis("2021-11-08T09:10:00"), results.get(1).windowEnd);
            assertEquals(10.044707429322814, results.get(1).ema38, 1e-12);
            assertEquals(0, harness.numEventTimeTimers());
        }
    }
}
