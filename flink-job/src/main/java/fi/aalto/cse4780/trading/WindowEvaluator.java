package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculator.ALPHA_100;
import static fi.aalto.cse4780.trading.EmaCalculator.ALPHA_38;
import static fi.aalto.cse4780.trading.EmaCalculator.WINDOW_MILLIS;

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

/**
 * Evaluates one symbol's windows in order: Query 1's EMA update and Query 2's crossover check,
 * with their metrics. Both {@link Variant.Operator}s evaluate through it.
 */
final class WindowEvaluator {

    static final ValueStateDescriptor<EmaState> EMAS =
            new ValueStateDescriptor<>("emas", TypeInformation.of(EmaState.class));
    static final ValueStateDescriptor<LastWindow> LAST_WINDOW =
            new ValueStateDescriptor<>("last-window", TypeInformation.of(LastWindow.class));

    private final Variant.EmptyWindows emptyWindows;
    private final Counter evaluatedWindows;
    private final Counter buyCrossovers;
    private final Counter sellCrossovers;
    private volatile long emaStateCount;

    WindowEvaluator(Variant.EmptyWindows emptyWindows, MetricGroup metrics) {
        this.emptyWindows = emptyWindows;
        evaluatedWindows = metrics.counter("evaluatedWindows");
        buyCrossovers = metrics.counter("buyCrossovers");
        sellCrossovers = metrics.counter("sellCrossovers");
        metrics.gauge("emaStates", () -> emaStateCount);
    }

    /** Whether {@link #evaluate} needs the {@link #LAST_WINDOW} state. */
    boolean carriesForward() {
        return emptyWindows == Variant.EmptyWindows.CARRY_FORWARD;
    }

    /**
     * Emits the result of the current key's window ending at {@code windowEnd}, after the empty
     * windows before it when it {@link #carriesForward()}. Carried windows repeat the previous
     * close and are published with this window, so they carry its {@code maxIngestTs} (D-42).
     *
     * @param lastWindow read only when it {@link #carriesForward()}, and may be null otherwise
     */
    void evaluate(String symbol, CloseCandidate close, long windowEnd, ValueState<EmaState> emas,
            ValueState<LastWindow> lastWindow, Collector<EmaResult> out) throws IOException {
        EmaState state = emas.value();
        if (state == null) {
            state = new EmaState();
            emaStateCount++;
        }
        if (carriesForward()) {
            LastWindow last = lastWindow.value();
            if (last != null) {
                for (long end = last.windowEnd + WINDOW_MILLIS; end < windowEnd; end += WINDOW_MILLIS) {
                    out.collect(advance(symbol, state, last.secType, end, last.close, close.maxIngestTimeMillis));
                }
            }
            lastWindow.update(new LastWindow(windowEnd, close.price, close.secType));
        }
        EmaResult result =
                advance(symbol, state, close.secType, windowEnd, close.price, close.maxIngestTimeMillis);
        emas.update(state);
        out.collect(result);
    }

    private EmaResult advance(String symbol, EmaState emas, String secType, long windowEnd, double close,
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
}
