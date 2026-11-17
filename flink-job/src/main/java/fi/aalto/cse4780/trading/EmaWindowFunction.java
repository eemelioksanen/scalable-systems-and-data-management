package fi.aalto.cse4780.trading;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.KeyedStateStore;
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

    public EmaWindowFunction(Variant.EmptyWindows emptyWindows) {
        this.emptyWindows = emptyWindows;
    }

    @Override
    public void open(OpenContext openContext) {
        evaluator = new WindowEvaluator(emptyWindows, getRuntimeContext().getMetricGroup());
    }

    @Override
    public void process(String symbol, Context context, Iterable<CloseCandidate> closes,
            Collector<EmaResult> out) throws Exception {
        KeyedStateStore keyed = context.globalState();
        evaluator.evaluate(symbol, closes.iterator().next(), context.window().getEnd(),
                keyed.getState(WindowEvaluator.EMAS),
                evaluator.carriesForward() ? keyed.getState(WindowEvaluator.LAST_WINDOW) : null, out);
    }
}
