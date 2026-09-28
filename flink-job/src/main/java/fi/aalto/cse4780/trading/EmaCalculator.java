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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Query 1: per symbol, picks each 5-minute window's close and updates EMA38 and EMA100.
 *
 * <p>Keyed by symbol. Out-of-order arrival can leave several windows open at once, so close
 * candidates are kept per window end. Each open window has an event-time timer on its last
 * millisecond. The timer fires once the watermark shows the next window has started (spec §3.1
 * (7)). Flink fires a key's timers in timestamp order, so windows are evaluated chronologically.
 *
 * <p>A window with no price events gets no timer, so it produces no result and leaves the EMAs
 * unchanged (D-6). EMA state is never reset, so it carries over across days (D-8).
 */
public class EmaCalculator extends KeyedProcessFunction<String, Tick, EmaResult> {

    private static final long serialVersionUID = 1L;

    public static final long WINDOW_MILLIS = Duration.ofMinutes(5).toMillis();

    static final double ALPHA_38 = 2.0 / (1 + 38);
    static final double ALPHA_100 = 2.0 / (1 + 100);

    /** Price events whose window had already been evaluated when they arrived. */
    public static final OutputTag<Tick> LATE_TICKS = new OutputTag<>("late-ticks") {};

    private transient MapState<Long, CloseCandidate> openWindows;
    private transient ValueState<EmaState> emas;
    private transient Counter lateTicks;

    @Override
    public void open(OpenContext openContext) {
        openWindows = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "open-windows", Types.LONG, TypeInformation.of(CloseCandidate.class)));
        emas = getRuntimeContext().getState(
                new ValueStateDescriptor<>("emas", TypeInformation.of(EmaState.class)));
        lateTicks = getRuntimeContext().getMetricGroup().counter("lateTicks");
    }

    @Override
    public void processElement(Tick tick, Context ctx, Collector<EmaResult> out) throws Exception {
        long windowEnd = windowEnd(tick.eventTimeMillis());
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

        EmaState state = emas.value();
        if (state == null) {
            state = new EmaState();
        }
        state.ema38 = close.price * ALPHA_38 + state.ema38 * (1 - ALPHA_38);
        state.ema100 = close.price * ALPHA_100 + state.ema100 * (1 - ALPHA_100);
        emas.update(state);

        out.collect(new EmaResult(
                ctx.getCurrentKey(),
                close.secType,
                windowEnd,
                close.price,
                state.ema38,
                state.ema100,
                close.maxIngestTimeMillis,
                System.currentTimeMillis()));
    }

    /** Exclusive end of the 5-minute window containing {@code eventTimeMillis}. */
    static long windowEnd(long eventTimeMillis) {
        return Math.floorDiv(eventTimeMillis, WINDOW_MILLIS) * WINDOW_MILLIS + WINDOW_MILLIS;
    }
}
