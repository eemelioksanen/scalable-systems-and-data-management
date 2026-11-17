package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Runs the pipeline (watermarks → keyBy → EmaCalculator → subscription filter) on a mini cluster. */
class TradingTrendsJobTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    @ParameterizedTest
    @EnumSource(Variant.Operator.class)
    void finiteInputEvaluatesEveryWindowIncludingTheLast(Variant.Operator operator) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tick> input = List.of(
                tick("AAA.FR", "2021-11-08T09:01:00", 20.0, 0),
                tick("BBB.NL", "2021-11-08T09:02:00", 30.0, 1),
                tick("AAA.FR", "2021-11-08T09:07:00", 21.0, 2),
                tick("AAA.FR", "2021-11-08T09:13:00", 22.0, 3),
                tick("BBB.NL", "2021-11-08T09:14:00", 33.0, 4));
        DataStream<Tick> ticks = env
                .fromData(input, TypeInformation.of(Tick.class))
                .assignTimestampsAndWatermarks(
                        TradingTrendsJob.watermarks(Duration.ZERO, Duration.ZERO))
                .setParallelism(1);

        List<EmaResult> results = new ArrayList<>();
        try (CloseableIterator<EmaResult> it =
                TradingTrendsJob.emaResults(ticks, baselineWith(operator)).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        // The 09:10-09:15 windows are only complete at end of input, when the watermark jumps to
        // the maximum. Per-symbol output order is chronological.
        Map<String, List<EmaResult>> bySymbol =
                results.stream().collect(Collectors.groupingBy(r -> r.symbol));
        assertWindows(bySymbol.get("AAA.FR"),
                new String[] {"09:05", "09:10", "09:15"},
                new double[] {1.0256410256410255, 2.0499671268902038, 3.073045735767629},
                new double[] {0.39603960396039606, 0.8040388197235565, 1.223760823293387});
        assertWindows(bySymbol.get("BBB.NL"),
                new String[] {"09:05", "09:15"},
                new double[] {1.5384615384615385, 3.1518737672583828},
                new double[] {0.594059405940594, 1.2357611998823645});
    }

    @ParameterizedTest
    @EnumSource(Variant.Operator.class)
    void endOfInputEvaluatesEveryWindowAlthoughTheSourceIsIdle(Variant.Operator operator) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // The source as the idleness timeout leaves it after a silence: idle. Flink then drops
        // the maximum watermark the source sends at end of input, so no watermark arrives at all.
        WatermarkStrategy<Long> idle = WatermarkStrategy
                .<Long>forGenerator(context -> new IdleGenerator())
                .withTimestampAssigner((index, recordTimestamp) ->
                        IDLE_SOURCE_INPUT[index.intValue()].eventTimeMillis);
        DataStream<Tick> ticks = env
                .fromSource(new NumberSequenceSource(0, IDLE_SOURCE_INPUT.length - 1), idle, "ticks")
                .setParallelism(1)
                .map(index -> IDLE_SOURCE_INPUT[index.intValue()])
                .returns(TypeInformation.of(Tick.class))
                .setParallelism(1);

        List<EmaResult> results = new ArrayList<>();
        try (CloseableIterator<EmaResult> it =
                TradingTrendsJob.emaResults(ticks, baselineWith(operator)).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        Map<String, List<EmaResult>> bySymbol =
                results.stream().collect(Collectors.groupingBy(r -> r.symbol));
        assertWindows(bySymbol.get("AAA.FR"),
                new String[] {"09:05", "09:10", "09:15"},
                new double[] {1.0256410256410255, 2.0499671268902038, 3.073045735767629},
                new double[] {0.39603960396039606, 0.8040388197235565, 1.223760823293387});
        assertWindows(bySymbol.get("BBB.NL"),
                new String[] {"09:05", "09:15"},
                new double[] {1.5384615384615385, 3.1518737672583828},
                new double[] {0.594059405940594, 1.2357611998823645});
    }

    @Test
    void publishesOnlySubscribedSymbolsWithTheirCrossovers() throws Exception {
        List<EmaResult> published = runWithSubscriptions();

        Map<String, List<EmaResult>> bySymbol =
                published.stream().collect(Collectors.groupingBy(r -> r.symbol));
        assertEquals(Set.of("AAA.FR", "CCC.ETR"), bySymbol.keySet());
        assertWindows(bySymbol.get("AAA.FR"),
                new String[] {"09:05", "09:10", "09:15"},
                new double[] {1.0256410256410255, 2.0499671268902038, 3.073045735767629},
                new double[] {0.39603960396039606, 0.8040388197235565, 1.223760823293387});
        assertEquals(Arrays.asList(Crossover.BUY, null, null),
                bySymbol.get("AAA.FR").stream().map(r -> r.crossover).collect(Collectors.toList()));
        assertEquals(List.of(Crossover.BUY),
                bySymbol.get("CCC.ETR").stream().map(r -> r.crossover).collect(Collectors.toList()));
    }

    @Test
    void aCleanRerunPublishesTheSameResults() throws Exception {
        assertEquals(comparable(runWithSubscriptions()), comparable(runWithSubscriptions()));
    }

    /**
     * Three symbols on three exchanges. BBB.NL's only subscriber is removed before the snapshot
     * ends and trader-3 subscribes to nothing, so AAA.FR and CCC.ETR are published.
     */
    private static List<EmaResult> runWithSubscriptions() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tick> input = List.of(
                tick("AAA.FR", "2021-11-08T09:01:00", 20.0, 0),
                tick("BBB.NL", "2021-11-08T09:02:00", 30.0, 1),
                tick("CCC.ETR", "2021-11-08T09:03:00", 40.0, 2),
                tick("AAA.FR", "2021-11-08T09:07:00", 21.0, 3),
                tick("AAA.FR", "2021-11-08T09:13:00", 22.0, 4),
                tick("BBB.NL", "2021-11-08T09:14:00", 33.0, 5));
        DataStream<Tick> ticks = env
                .fromData(input, TypeInformation.of(Tick.class))
                .assignTimestampsAndWatermarks(
                        TradingTrendsJob.watermarks(Duration.ZERO, Duration.ZERO))
                .setParallelism(1);
        List<SubscriptionUpdate> updates = List.of(
                new SubscriptionUpdate("trader-1", new String[] {"AAA.FR"}, 0, 0),
                new SubscriptionUpdate("trader-2", new String[] {"AAA.FR", "CCC.ETR"}, 0, 1),
                new SubscriptionUpdate("trader-3", new String[0], 0, 2),
                new SubscriptionUpdate("trader-4", new String[] {"BBB.NL"}, 0, 3),
                new SubscriptionUpdate("trader-4", null, 0, 4));
        DataStream<SubscriptionUpdate> subscriptions = env
                .fromData(updates, TypeInformation.of(SubscriptionUpdate.class))
                .setParallelism(1);

        List<EmaResult> results = new ArrayList<>();
        try (CloseableIterator<EmaResult> it = TradingTrendsJob.published(
                TradingTrendsJob.emaResults(ticks, Variant.BASELINE), subscriptions, Map.of(0, 5L))
                .executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        return results;
    }

    private static Variant baselineWith(Variant.Operator operator) {
        return new Variant(operator, Variant.BASELINE.emptyWindows(), Variant.BASELINE.stateBackend(),
                Variant.BASELINE.ticksEncoding());
    }

    /** Every published field except the wall-clock emit time, in a stable order. */
    private static List<String> comparable(List<EmaResult> results) {
        return results.stream()
                .map(r -> r.symbol + ' ' + r.secType + ' ' + r.windowEnd + ' ' + r.close + ' '
                        + r.ema38 + ' ' + r.ema100 + ' ' + r.maxIngestTs + ' ' + r.crossover)
                .sorted()
                .collect(Collectors.toList());
    }

    private static void assertWindows(
            List<EmaResult> results, String[] windowEnds, double[] ema38, double[] ema100) {
        assertEquals(windowEnds.length, results.size(), () -> "results: " + results);
        for (int i = 0; i < windowEnds.length; i++) {
            EmaResult r = results.get(i);
            assertEquals(millis("2021-11-08T" + windowEnds[i] + ":00"), r.windowEnd);
            assertEquals(ema38[i], r.ema38, 1e-12);
            assertEquals(ema100[i], r.ema100, 1e-12);
        }
    }

    private static Tick tick(String symbol, String localDateTime, double price, long order) {
        return new Tick(symbol, "E", price, millis(localDateTime), order, 0);
    }

    /** Static, so the source's functions reach it without serializing the ticks. */
    private static final Tick[] IDLE_SOURCE_INPUT = {
        tick("AAA.FR", "2021-11-08T09:01:00", 20.0, 0),
        tick("BBB.NL", "2021-11-08T09:02:00", 30.0, 1),
        tick("AAA.FR", "2021-11-08T09:07:00", 21.0, 2),
        tick("AAA.FR", "2021-11-08T09:13:00", 22.0, 3),
        tick("BBB.NL", "2021-11-08T09:14:00", 33.0, 4),
    };

    /** Emits no watermark and marks its output idle at every event. */
    private static final class IdleGenerator implements WatermarkGenerator<Long> {

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
            output.markIdle();
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
