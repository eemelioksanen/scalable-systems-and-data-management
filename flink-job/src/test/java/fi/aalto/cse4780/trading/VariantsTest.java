package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every variant of D-42 against its reference on the hand-derived fixtures. Skip and carry-forward
 * each have their own expectations in {@code fixtures/semantics/expected.json}; the other switches
 * must not change them.
 */
class VariantsTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path SEMANTICS = Path.of("..", "fixtures", "semantics", "expected.json");
    private static final Path GOLDEN = Path.of("..", "fixtures", "golden");

    /** Admits day 2 line 63, which needs 84,450,001 ms (fixtures/semantics/derivation.md). */
    private static final Duration ADMIT_ALL = Duration.ofDays(1);

    /** Fixture prices are at most 100, where binary64 carries about 1e-14. */
    private static final double EMA_TOLERANCE = 1e-12;

    /**
     * Every operator, empty-window policy and state backend. The two encodings give the same ticks
     * on these rows ({@link BinaryTicksContractTest}), so each combination runs with one of them,
     * picked by parity so that both meet every value of the other switches.
     */
    static Stream<Variant> variants() {
        List<Variant> all = new ArrayList<>();
        for (Variant.Operator operator : Variant.Operator.values()) {
            for (Variant.EmptyWindows emptyWindows : Variant.EmptyWindows.values()) {
                for (Variant.StateBackend backend : Variant.StateBackend.values()) {
                    int parity = (operator.ordinal() + emptyWindows.ordinal() + backend.ordinal()) % 2;
                    Variant.TicksEncoding encoding = Variant.TicksEncoding.values()[parity];
                    all.add(new Variant(operator, emptyWindows, backend, encoding));
                }
            }
        }
        return all.stream();
    }

    @ParameterizedTest
    @MethodSource("variants")
    void semanticsFixturesMatchTheVariantsReference(Variant variant) throws Exception {
        JsonNode expected = JSON.readTree(SEMANTICS.toFile());
        JsonNode reference = variant.emptyWindows() == Variant.EmptyWindows.SKIP
                ? expected
                : expected.get("variants").get("emptyWindowsCarryForward");

        List<Tick> ticks = new ArrayList<>();
        TickParser parser = new TickParser(ZoneOffset.UTC);
        for (BinaryTicksContractTest.Row row : BinaryTicksContractTest.rows()) {
            Tick tick = new Tick();
            TickParser.Outcome outcome = variant.ticksEncoding() == Variant.TicksEncoding.CSV
                    ? parser.parse(row.csv(), tick)
                    : parser.parseBinary(row.binary(), tick);
            if (outcome.isPrice()) {
                tick.sourceOrder = ticks.size();
                ticks.add(tick);
            }
        }

        List<EmaResult> got = run(variant, ticks);
        assertWindows(reference.get("windows"), got, variant.toString());
        assertAdvisories(reference.get("advisories"), got, variant.toString());
    }

    @ParameterizedTest
    @EnumSource(Variant.StateBackend.class)
    void theStateBackendSwitchReachesTheKeyedOperators(Variant.StateBackend stateBackend) throws Exception {
        Variant variant = new Variant(Variant.Operator.PROCESS, Variant.EmptyWindows.SKIP, stateBackend,
                Variant.TicksEncoding.CSV);
        StreamExecutionEnvironment env = TradingTrendsJob.environment(variant);
        env.setParallelism(1);
        List<String> backends = new ArrayList<>();
        try (CloseableIterator<String> it = env
                .fromData(List.of("AAA.FR", "BBB.NL"), Types.STRING)
                .keyBy(symbol -> symbol)
                .transform("backend", Types.STRING, new BackendProbe<>())
                .executeAndCollect()) {
            it.forEachRemaining(backends::add);
        }
        String expected = stateBackend == Variant.StateBackend.ROCKSDB ? "RocksDBKeyedStateBackend"
                : "HeapKeyedStateBackend";
        assertEquals(List.of(expected), backends);
    }

    @ParameterizedTest
    @EnumSource(Variant.EmptyWindows.class)
    void goldenSequencesMatchForBothOperators(Variant.EmptyWindows emptyWindows) throws Exception {
        for (String file : List.of("synthetic.json", "real.json")) {
            JsonNode golden = JSON.readTree(GOLDEN.resolve(file).toFile());
            if (emptyWindows == Variant.EmptyWindows.CARRY_FORWARD && file.equals("real.json")) {
                // Only the synthetic sequence has no empty windows, so only it shares the skip reference.
                continue;
            }
            String symbol = golden.get("symbol").asText();
            String secType = golden.get("secType").asText();
            List<Tick> ticks = new ArrayList<>();
            for (JsonNode window : golden.get("windows")) {
                long windowEnd = window.get("windowEnd").asLong();
                double close = window.get("close").asDouble();
                ticks.add(new Tick(symbol, secType, close, windowEnd - 1, ticks.size(), 0));
                ticks.add(new Tick(symbol, secType, close + 1, windowEnd - EmaCalculator.WINDOW_MILLIS,
                        ticks.size(), 0));
            }
            for (Variant.Operator operator : Variant.Operator.values()) {
                Variant variant = new Variant(operator, emptyWindows, Variant.StateBackend.HASHMAP,
                        Variant.TicksEncoding.CSV);
                List<EmaResult> got = run(variant, ticks);
                String at = file + " " + variant;
                assertEquals(golden.get("windows").size(), got.size(), at);
                for (int i = 0; i < got.size(); i++) {
                    JsonNode want = golden.get("windows").get(i);
                    assertEquals(want.get("windowEnd").asLong(), got.get(i).windowEnd, at);
                    assertEquals(want.get("close").asDouble(), got.get(i).close, at);
                    assertEquals(want.get("ema38").asDouble(), got.get(i).ema38, 1e-9, at);
                    assertEquals(want.get("ema100").asDouble(), got.get(i).ema100, 1e-9, at);
                }
                assertEquals(golden.get("advisories").size(),
                        got.stream().filter(r -> r.crossover != null).count(), at);
            }
        }
    }

    /** Both operators reject the same late event and publish the same windows without it. */
    @ParameterizedTest
    @EnumSource(Variant.EmptyWindows.class)
    void bothOperatorsRejectTheSameLateEvent(Variant.EmptyWindows emptyWindows) throws Exception {
        List<Tick> ticks = List.of(
                tick("AAA.FR", "2021-11-08T09:01:00", 20.0, 0),
                tick("AAA.FR", "2021-11-08T09:07:00", 21.0, 1),
                tick("AAA.FR", "2021-11-08T09:02:00", 99.0, 2),
                tick("AAA.FR", "2021-11-08T09:18:00", 22.0, 3));
        Map<Variant.Operator, List<String>> outputs = new java.util.EnumMap<>(Variant.Operator.class);
        for (Variant.Operator operator : Variant.Operator.values()) {
            Variant variant = new Variant(operator, emptyWindows, Variant.StateBackend.HASHMAP,
                    Variant.TicksEncoding.CSV);
            StreamExecutionEnvironment env = TradingTrendsJob.environment(variant);
            env.setParallelism(1);
            DataStream<Tick> stream = env
                    .fromData(ticks, TypeInformation.of(Tick.class))
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Tick>forGenerator(context -> new EveryEvent())
                            .withTimestampAssigner((t, ts) -> t.eventTimeMillis));
            SingleOutputStreamOperator<EmaResult> results = TradingTrendsJob.emaResults(stream, variant);
            List<String> lines = new ArrayList<>();
            try (CloseableIterator<String> it = results
                    .map(r -> "result " + r.windowEnd + " " + r.close + " " + r.ema38 + " " + r.crossover)
                    .union(results.getSideOutput(EmaCalculator.LATE_TICKS).map(t -> "late " + t.sourceOrder))
                    .executeAndCollect()) {
                it.forEachRemaining(lines::add);
            }
            lines.sort(Comparator.naturalOrder());
            outputs.put(operator, lines);
        }
        List<String> process = outputs.get(Variant.Operator.PROCESS);
        assertEquals(List.of("late 2"), process.stream().filter(l -> l.startsWith("late")).toList());
        assertEquals(emptyWindows == Variant.EmptyWindows.SKIP ? 3 : 4,
                process.stream().filter(l -> l.startsWith("result")).count(), process::toString);
        assertEquals(process, outputs.get(Variant.Operator.WINDOW));
    }

    private static List<EmaResult> run(Variant variant, List<Tick> ticks) throws Exception {
        StreamExecutionEnvironment env = TradingTrendsJob.environment(variant);
        env.setParallelism(2);
        DataStream<Tick> stream = env
                .fromData(ticks, TypeInformation.of(Tick.class))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(TradingTrendsJob.watermarks(ADMIT_ALL, Duration.ZERO))
                .setParallelism(1);
        List<EmaResult> results = new ArrayList<>();
        try (CloseableIterator<EmaResult> it = TradingTrendsJob.emaResults(stream, variant).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        results.sort(Comparator.<EmaResult, String>comparing(r -> r.symbol).thenComparingLong(r -> r.windowEnd));
        return results;
    }

    private static void assertWindows(JsonNode expected, List<EmaResult> got, String at) {
        List<JsonNode> want = new ArrayList<>();
        expected.forEach(want::add);
        want.sort(Comparator.<JsonNode, String>comparing(w -> w.get("symbol").asText())
                .thenComparingLong(w -> w.get("windowEnd").asLong()));
        assertEquals(want.size(), got.size(), at);
        for (int i = 0; i < want.size(); i++) {
            JsonNode w = want.get(i);
            EmaResult r = got.get(i);
            String window = at + " " + w;
            assertEquals(w.get("symbol").asText(), r.symbol, window);
            assertEquals(w.get("secType").asText(), r.secType, window);
            assertEquals(w.get("windowEnd").asLong(), r.windowEnd, window);
            assertEquals(w.get("close").asDouble(), r.close, window);
            assertEquals(w.get("ema38").asDouble(), r.ema38, EMA_TOLERANCE, window);
            assertEquals(w.get("ema100").asDouble(), r.ema100, EMA_TOLERANCE, window);
        }
    }

    private static void assertAdvisories(JsonNode expected, List<EmaResult> got, String at) {
        List<String> want = new ArrayList<>();
        expected.forEach(a -> want.add(a.get("symbol").asText() + " " + a.get("windowEnd").asLong() + " "
                + a.get("type").asText()));
        List<String> actual = got.stream()
                .filter(r -> r.crossover != null)
                .map(r -> r.symbol + " " + r.windowEnd + " " + r.crossover)
                .toList();
        assertEquals(want.stream().sorted().toList(), actual.stream().sorted().toList(), at);
    }

    private static Tick tick(String symbol, String localDateTime, double price, long order) {
        return new Tick(symbol, "E", price, millis(localDateTime), order, 0);
    }

    /** A watermark one millisecond behind every event, so lateness does not depend on timing. */
    private static final class EveryEvent implements WatermarkGenerator<Tick> {

        @Override
        public void onEvent(Tick event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventTimestamp - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    /** Emits, for its first record, the class name of the keyed state backend it runs on. */
    private static final class BackendProbe<T> extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<T, String> {

        private transient boolean reported;

        @Override
        public void processElement(StreamRecord<T> element) {
            if (!reported) {
                reported = true;
                output.collect(new StreamRecord<>(getKeyedStateBackend().getClass().getSimpleName()));
            }
        }
    }
}
