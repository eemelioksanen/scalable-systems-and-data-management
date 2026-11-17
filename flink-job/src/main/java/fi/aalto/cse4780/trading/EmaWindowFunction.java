package fi.aalto.cse4780.trading;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The Window API variant of Query 1 and Query 2 (D-42). Flink fires a key's windows in time order,
 * and the EMAs live in the key's global state, so each firing continues from the previous window,
 * as {@link EmaCalculator}'s timers do.
 */
public class EmaWindowFunction
        extends ProcessWindowFunction<CloseCandidate, EmaResult, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final Variant.EmptyWindows emptyWindows;

    private transient WindowEvaluator evaluator;
    private transient volatile long emaStateCount;

    public EmaWindowFunction(Variant.EmptyWindows emptyWindows) {
        this.emptyWindows = emptyWindows;
    }

    @Override
    public void open(OpenContext openContext) {
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        evaluator = new WindowEvaluator(metrics);
        metrics.gauge("emaStates", () -> emaStateCount);
    }

    @Override
    public void process(String symbol, Context context, Iterable<CloseCandidate> closes,
            Collector<EmaResult> out) throws Exception {
        CloseCandidate close = closes.iterator().next();
        long windowEnd = context.window().getEnd();
        KeyedStateStore keyed = context.globalState();
        ValueState<EmaState> emas = keyed.getState(EmaCalculator.EMAS);
        EmaState state = emas.value();
        if (state == null) {
            state = new EmaState();
            emaStateCount++;
        }
        if (emptyWindows == Variant.EmptyWindows.CARRY_FORWARD) {
            ValueState<LastWindow> lastWindow = keyed.getState(EmaCalculator.LAST_WINDOW);
            LastWindow last = lastWindow.value();
            if (last != null) {
                evaluator.carryForward(symbol, state, last, windowEnd, close.maxIngestTimeMillis, out);
            }
            lastWindow.update(new LastWindow(windowEnd, close.price, close.secType));
        }
        EmaResult result = evaluator.evaluate(symbol, state, close.secType, windowEnd, close.price,
                close.maxIngestTimeMillis);
        emas.update(state);
        out.collect(result);
    }
}
