package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SubscriptionFilterTest {

    private KeyedBroadcastOperatorTestHarness<String, EmaResult, SubscriptionUpdate, EmaResult>
            harness;
    private long nextWindowEnd = millis("2021-11-08T09:05:00");

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void holdsResultsUntilTheStartupSubscriptionsAreRead() throws Exception {
        start(Map.of(0, 2L));
        EmaResult a = result("A.FR");
        harness.processElement(a, 0);
        subscribe(0, "trader-1", "A.FR");
        assertEquals(List.of(), published());

        subscribe(1, "trader-2", "B.NL");
        assertEquals(ids(a), ids(published()));
    }

    @Test
    void releasesHeldResultsInOrderAndOnlyForSubscribedSymbols() throws Exception {
        start(Map.of(0, 1L));
        EmaResult a1 = result("A.FR");
        EmaResult b = result("B.NL");
        EmaResult a2 = result("A.FR");
        harness.processElement(a1, 0);
        harness.processElement(b, 0);
        harness.processElement(a2, 0);

        subscribe(0, "trader-1", "A.FR");
        assertEquals(ids(a1, a2), ids(published()));
    }

    @Test
    void waitsForEveryPartitionThatHeldRecords() throws Exception {
        start(Map.of(0, 1L, 1, 2L));
        harness.processElement(result("A.FR"), 0);
        subscribe(0, "trader-1", "A.FR");
        harness.processBroadcastElement(new SubscriptionUpdate("trader-2", new String[0], 1, 0), 0);
        assertEquals(0, published().size());

        harness.processBroadcastElement(new SubscriptionUpdate("trader-3", new String[0], 1, 1), 0);
        assertEquals(1, published().size());
    }

    @Test
    void malformedRecordsStillCountTowardsReadiness() throws Exception {
        start(Map.of(0, 2L));
        harness.processElement(result("A.FR"), 0);
        subscribe(0, "trader-1", "A.FR");
        harness.processBroadcastElement(new SubscriptionUpdate(null, null, 0, 1), 0);

        assertEquals(1, published().size());
    }

    @Test
    void anEmptyTopicIsReadyAtOnce() throws Exception {
        start(Map.of());
        harness.processElement(result("A.FR"), 0);
        subscribe(0, "trader-1", "A.FR");
        EmaResult next = result("A.FR");
        harness.processElement(next, 0);

        assertEquals(ids(next), ids(published()));
    }

    @Test
    void theWildcardPublishesEverySymbol() throws Exception {
        start(Map.of());
        subscribe(0, "trader-1", SubscriptionFilter.ALL_SYMBOLS);
        harness.processElement(result("A.FR"), 0);
        harness.processElement(result("B.NL"), 0);
        harness.processElement(result("846741.ETR"), 0);

        assertEquals(3, published().size());
    }

    @Test
    void aSymbolStaysPublishedUntilItsLastSubscriberLeaves() throws Exception {
        start(Map.of());
        subscribe(0, "trader-1", "A.FR", "B.NL");
        subscribe(1, "trader-2", "B.NL", "C.ETR");
        remove(2, "trader-1");
        assertEquals(List.of("B.NL", "C.ETR"), publishedSymbolsOf("A.FR", "B.NL", "C.ETR"));

        subscribe(3, "trader-2");
        assertEquals(List.of(), publishedSymbolsOf("A.FR", "B.NL", "C.ETR"));
    }

    @Test
    void aNewListReplacesTheTradersOldOne() throws Exception {
        start(Map.of());
        subscribe(0, "trader-1", "A.FR");
        subscribe(1, "trader-1", "B.NL");

        assertEquals(List.of("B.NL"), publishedSymbolsOf("A.FR", "B.NL"));
    }

    @Test
    void aSymbolListedTwiceIsDroppedWithItsTrader() throws Exception {
        start(Map.of());
        subscribe(0, "trader-1", "A.FR", "A.FR");
        subscribe(1, "trader-2", "A.FR");
        remove(2, "trader-2");
        assertEquals(List.of("A.FR"), publishedSymbolsOf("A.FR"));

        remove(3, "trader-1");
        assertEquals(List.of(), publishedSymbolsOf("A.FR"));
    }

    @Test
    void aLateSubscriptionPublishesEmasBuiltFromTheWholeHistory() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Tick, EmaResult> emas =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new EmaCalculator(), tick -> tick.symbol, Types.STRING);
        try {
            start(Map.of());
            emas.processElement(new Tick("A.FR", "E", 100.0, millis("2021-11-08T09:01:00"), 0, 0),
                    0);
            emas.processWatermark(millis("2021-11-08T09:05:00"));
            forward(emas);
            assertEquals(List.of(), published());

            subscribe(0, "trader-1", "A.FR");
            emas.processElement(new Tick("A.FR", "E", 101.0, millis("2021-11-08T09:06:00"), 1, 0),
                    0);
            emas.processWatermark(millis("2021-11-08T09:10:00"));
            forward(emas);

            // The second window's EMAs, as in EmaCalculatorTest: they include the unpublished first.
            List<EmaResult> published = published();
            assertEquals(1, published.size());
            assertEquals(millis("2021-11-08T09:10:00"), published.get(0).windowEnd);
            assertEquals(10.044707429322814, published.get(0).ema38, 1e-12);
            assertEquals(3.940986177825703, published.get(0).ema100, 1e-12);
        } finally {
            emas.close();
        }
    }

    @Test
    void restoresHeldResultsAndSubscriptionsFromASnapshot() throws Exception {
        Map<Integer, Long> startupEnd = Map.of(0, 2L);
        start(startupEnd);
        subscribe(0, "trader-1", "A.FR");
        EmaResult held = result("A.FR");
        harness.processElement(held, 0);
        OperatorSubtaskState notReady = harness.snapshot(1, 0);
        harness.close();

        harness = restore(startupEnd, notReady);
        subscribe(1, "trader-2", "B.NL");
        assertEquals(ids(held), ids(published()));
        OperatorSubtaskState ready = harness.snapshot(2, 0);
        harness.close();

        harness = restore(startupEnd, ready);
        EmaResult a = result("A.FR");
        harness.processElement(a, 0);
        harness.processElement(result("C.ETR"), 0);
        assertEquals(ids(a), ids(published()));
    }

    private void start(Map<Integer, Long> startupEndOffsets) throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                new SubscriptionFilter(startupEndOffsets), result -> result.symbol, Types.STRING,
                SubscriptionFilter.TRADERS, SubscriptionFilter.SUBSCRIBED,
                SubscriptionFilter.POSITION);
    }

    private static KeyedBroadcastOperatorTestHarness<String, EmaResult, SubscriptionUpdate, EmaResult>
            restore(Map<Integer, Long> startupEndOffsets, OperatorSubtaskState state)
            throws Exception {
        KeyedBroadcastOperatorTestHarness<String, EmaResult, SubscriptionUpdate, EmaResult> restored =
                new KeyedBroadcastOperatorTestHarness<>(
                        new CoBroadcastWithKeyedOperator<>(
                                new SubscriptionFilter(startupEndOffsets),
                                List.of(SubscriptionFilter.TRADERS, SubscriptionFilter.SUBSCRIBED,
                                        SubscriptionFilter.POSITION)),
                        result -> result.symbol, Types.STRING, 1, 1, 0);
        restored.initializeState(state);
        restored.open();
        return restored;
    }

    private void subscribe(long offset, String trader, String... symbols) throws Exception {
        harness.processBroadcastElement(new SubscriptionUpdate(trader, symbols, 0, offset), 0);
    }

    private void remove(long offset, String trader) throws Exception {
        harness.processBroadcastElement(new SubscriptionUpdate(trader, null, 0, offset), 0);
    }

    private void forward(KeyedOneInputStreamOperatorTestHarness<String, Tick, EmaResult> emas)
            throws Exception {
        for (EmaResult result : emas.extractOutputValues()) {
            harness.processElement(result, 0);
        }
        emas.getOutput().clear();
    }

    /** Sends one result per symbol and returns the symbols published, clearing the output. */
    private List<String> publishedSymbolsOf(String... symbols) throws Exception {
        for (String symbol : symbols) {
            harness.processElement(result(symbol), 0);
        }
        return published().stream().map(r -> r.symbol).collect(Collectors.toList());
    }

    /** The results published since the last call. */
    private List<EmaResult> published() {
        List<EmaResult> out = new ArrayList<>(harness.extractOutputValues());
        harness.getOutput().clear();
        return out;
    }

    /** Symbol and window of each result: the harness copies records, so they are compared by these. */
    private static List<String> ids(List<EmaResult> results) {
        return results.stream().map(r -> r.symbol + '@' + r.windowEnd).collect(Collectors.toList());
    }

    private static List<String> ids(EmaResult... results) {
        return ids(List.of(results));
    }

    private EmaResult result(String symbol) {
        EmaResult result = new EmaResult(symbol, "E", nextWindowEnd, 1, 2, 3, 0, 0);
        nextWindowEnd += EmaCalculator.WINDOW_MILLIS;
        return result;
    }
}
