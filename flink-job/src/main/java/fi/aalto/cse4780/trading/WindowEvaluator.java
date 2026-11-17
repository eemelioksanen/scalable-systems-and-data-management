package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculator.ALPHA_100;
import static fi.aalto.cse4780.trading.EmaCalculator.ALPHA_38;
import static fi.aalto.cse4780.trading.EmaCalculator.WINDOW_MILLIS;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

/**
 * Evaluates one symbol's windows in order: Query 1's EMA update and Query 2's crossover check,
 * with their counters. Both {@link Variant.Operator}s evaluate through it.
 */
final class WindowEvaluator {

    private final Counter evaluatedWindows;
    private final Counter buyCrossovers;
    private final Counter sellCrossovers;

    WindowEvaluator(MetricGroup metrics) {
        evaluatedWindows = metrics.counter("evaluatedWindows");
        buyCrossovers = metrics.counter("buyCrossovers");
        sellCrossovers = metrics.counter("sellCrossovers");
    }

    /** Advances {@code emas} by the window ending at {@code windowEnd} and returns its result. */
    EmaResult evaluate(String symbol, EmaState emas, String secType, long windowEnd, double close,
            long maxIngestTs) {
        double previousEma38 = emas.ema38;
        double previousEma100 = emas.ema100;
        emas.ema38 = close * ALPHA_38 + emas.ema38 * (1 - ALPHA_38);
        emas.ema100 = close * ALPHA_100 + emas.ema100 * (1 - ALPHA_100);

        EmaResult result = new EmaResult(symbol, secType, windowEnd, close, emas.ema38, emas.ema100,
                maxIngestTs, System.currentTimeMillis());
        result.crossover = Crossover.detect(previousEma38, previousEma100, emas.ema38, emas.ema100);
        evaluatedWindows.inc();
        if (result.crossover == Crossover.BUY) {
            buyCrossovers.inc();
        } else if (result.crossover == Crossover.SELL) {
            sellCrossovers.inc();
        }
        return result;
    }

    /**
     * {@link Variant.EmptyWindows#CARRY_FORWARD}: evaluates each empty window between {@code last}
     * and {@code windowEnd} on {@code last}'s close. They are published with the window that ends
     * the gap, so they carry its {@code maxIngestTs}.
     */
    void carryForward(String symbol, EmaState emas, LastWindow last, long windowEnd, long maxIngestTs,
            Collector<EmaResult> out) {
        for (long end = last.windowEnd + WINDOW_MILLIS; end < windowEnd; end += WINDOW_MILLIS) {
            out.collect(evaluate(symbol, emas, last.secType, end, last.close, maxIngestTs));
        }
    }
}
