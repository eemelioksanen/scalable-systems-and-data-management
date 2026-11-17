package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Replays the hand-worked sequences in {@code fixtures/golden} (see derivation.md there) through
 * the operator. Each listed window gets its close as a trade on the window's last millisecond, plus
 * an earlier, differently priced trade that arrives after it and must not become the close.
 */
class GoldenFixturesTest {

    private static final Path GOLDEN = Path.of("..", "fixtures", "golden");

    /** The oracle comparison tolerance (D-18). */
    private static final double TOLERANCE = 1e-9;

    @ParameterizedTest
    @ValueSource(strings = {"synthetic.json", "real.json"})
    void reproducesTheHandWorkedEmasAndAdvisories(String file) throws Exception {
        Path path = GOLDEN.resolve(file);
        assertTrue(Files.exists(path), () -> "missing fixture " + path.toAbsolutePath());
        JsonNode golden = new ObjectMapper().readTree(path.toFile());

        assertEquals(EmaCalculator.ALPHA_38, golden.get("alpha38").asDouble());
        assertEquals(EmaCalculator.ALPHA_100, golden.get("alpha100").asDouble());
        assertEquals(EmaCalculator.WINDOW_MILLIS, golden.get("windowMs").asLong());
        assertEquals(0.0, golden.get("initialEma").asDouble());

        String symbol = golden.get("symbol").asText();
        String secType = golden.get("secType").asText();
        JsonNode windows = golden.get("windows");

        KeyedOneInputStreamOperatorTestHarness<String, Tick, EmaResult> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new EmaCalculator(Variant.EmptyWindows.SKIP), tick -> tick.symbol, Types.STRING);
        try {
            long order = 0;
            for (JsonNode window : windows) {
                long windowEnd = window.get("windowEnd").asLong();
                double close = window.get("close").asDouble();
                long lastMillis = windowEnd - 1;
                long firstMillis = windowEnd - EmaCalculator.WINDOW_MILLIS;
                harness.processElement(
                        new Tick(symbol, secType, close, lastMillis, order++, 0), lastMillis);
                harness.processElement(
                        new Tick(symbol, secType, close + 1, firstMillis, order++, 0), firstMillis);
            }
            harness.processWatermark(Long.MAX_VALUE);

            List<EmaResult> results = harness.extractOutputValues();
            assertEquals(windows.size(), results.size());
            for (int i = 0; i < windows.size(); i++) {
                JsonNode expected = windows.get(i);
                EmaResult actual = results.get(i);
                String at = file + " window " + (i + 1);
                assertEquals(symbol, actual.symbol, at);
                assertEquals(secType, actual.secType, at);
                assertEquals(expected.get("windowEnd").asLong(), actual.windowEnd, at);
                assertEquals(expected.get("close").asDouble(), actual.close, at);
                assertEquals(expected.get("ema38").asDouble(), actual.ema38, TOLERANCE, at);
                assertEquals(expected.get("ema100").asDouble(), actual.ema100, TOLERANCE, at);
            }

            List<EmaResult> crossovers =
                    results.stream().filter(r -> r.crossover != null).collect(Collectors.toList());
            JsonNode advisories = golden.get("advisories");
            assertEquals(advisories.size(), crossovers.size(), () -> file + ": " + crossovers);
            for (int i = 0; i < advisories.size(); i++) {
                JsonNode expected = advisories.get(i);
                EmaResult actual = crossovers.get(i);
                String at = file + " advisory " + (i + 1);
                assertEquals(expected.get("windowEnd").asLong(), actual.windowEnd, at);
                assertEquals(expected.get("type").asText(), actual.crossover.name(), at);
                assertEquals(expected.get("ema38").asDouble(), actual.ema38, TOLERANCE, at);
                assertEquals(expected.get("ema100").asDouble(), actual.ema100, TOLERANCE, at);
            }
        } finally {
            harness.close();
        }
    }
}
