package fi.aalto.cse4780.trading;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Query 1: per symbol, picks each 5-minute window's close and updates EMA38 and EMA100. Query 2:
 * compares them with the symbol's previous EMAs and attaches any {@link Crossover} to the result.
 *
 * <p>Both run for every symbol. Subscriptions only decide what is published, downstream (D-9).
 *
 * <p>Keyed by symbol. Out-of-order arrival can leave several windows open at once, so close
 * candidates are kept per window end. Each open window has an event-time timer on its last
 * millisecond. The timer fires once the watermark shows the next window has started (spec §3.1
 * (7)). Flink fires a key's timers in timestamp order, so windows are evaluated chronologically.
 *
 * <p>A window with no price events gets no timer, so it produces no result and leaves the EMAs
 * unchanged (D-6). It is not a crossover check's previous window either. With
 * {@link Variant.EmptyWindows#CARRY_FORWARD}, the next window's timer first evaluates the empty
 * windows before it on the previous close (D-42). EMA state is never reset, so it carries over
 * across days (D-8).
 */
public class EmaCalculator extends KeyedProcessFunction<String, Tick, EmaResult> {

    private static final long serialVersionUID = 1L;

    public static final long WINDOW_MILLIS = Duration.ofMinutes(5).toMillis();

    static final double ALPHA_38 = 2.0 / (1 + 38);
    static final double ALPHA_100 = 2.0 / (1 + 100);

    /** Price events whose window had already been evaluated when they arrived. */
    public static final OutputTag<Tick> LATE_TICKS = new OutputTag<>("late-ticks") {};

    static final ValueStateDescriptor<EmaState> EMAS =
            new ValueStateDescriptor<>("emas", TypeInformation.of(EmaState.class));
    static final ValueStateDescriptor<LastWindow> LAST_WINDOW =
            new ValueStateDescriptor<>("last-window", TypeInformation.of(LastWindow.class));

    private final Variant.EmptyWindows emptyWindows;

    private transient MapState<Long, CloseCandidate> openWindows;
    private transient ValueState<EmaState> emas;
    private transient ValueState<LastWindow> lastWindow;
    private transient WindowEvaluator evaluator;
    private transient Counter lateTicks;
    private transient volatile long emaStateCount;
    private transient volatile long openWindowCount;

    public EmaCalculator() {
        this(Variant.EmptyWindows.SKIP);
    }

    public EmaCalculator(Variant.EmptyWindows emptyWindows) {
        this.emptyWindows = emptyWindows;
    }

    @Override
    public void open(OpenContext openContext) {
        openWindows = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "open-windows", Types.LONG, TypeInformation.of(CloseCandidate.class)));
        emas = getRuntimeContext().getState(EMAS);
        if (emptyWindows == Variant.EmptyWindows.CARRY_FORWARD) {
            lastWindow = getRuntimeContext().getState(LAST_WINDOW);
        }
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        lateTicks = metrics.counter("lateTicks");
        evaluator = new WindowEvaluator(metrics);
        metrics.gauge("emaStates", () -> emaStateCount);
        metrics.gauge("openWindows", () -> openWindowCount);
    }

    @Override
    public void processElement(Tick tick, Context ctx, Collector<EmaResult> out) throws Exception {
        long windowEnd = windowEnd(tick.eventTimeMillis);
        long lastMillis = windowEnd - 1;
        if (lastMillis <= ctx.timerService().currentWatermark()) {
            lateTicks.inc();
            ctx.output(LATE_TICKS, tick);
            return;
        }

        CloseCandidate candidate = openWindows.get(windowEnd);
        if (candidate == null) {
            candidate = new CloseCandidate(tick);
            ctx.timerService().registerEventTimeTimer(lastMillis);
            openWindowCount++;
        } else {
            candidate.offer(tick);
        }
        openWindows.put(windowEnd, candidate);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EmaResult> out)
            throws Exception {
        long windowEnd = timestamp + 1;
        CloseCandidate close = openWindows.get(windowEnd);
        openWindows.remove(windowEnd);
        openWindowCount--;

        EmaState state = emas.value();
        if (state == null) {
            state = new EmaState();
            emaStateCount++;
        }
        String symbol = ctx.getCurrentKey();
        if (lastWindow != null) {
            LastWindow last = lastWindow.value();
            if (last != null) {
                evaluator.carryForward(
                        symbol, state, last, windowEnd, close.maxIngestTimeMillis, out);
            }
            lastWindow.update(new LastWindow(windowEnd, close.price, close.secType));
        }
        EmaResult result = evaluator.evaluate(symbol, state, close.secType, windowEnd, close.price,
                close.maxIngestTimeMillis);
        emas.update(state);
        out.collect(result);
    }

    /** Exclusive end of the 5-minute window containing {@code eventTimeMillis}. */
    static long windowEnd(long eventTimeMillis) {
        return Math.floorDiv(eventTimeMillis, WINDOW_MILLIS) * WINDOW_MILLIS + WINDOW_MILLIS;
    }
}
