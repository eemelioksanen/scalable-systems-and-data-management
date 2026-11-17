package fi.aalto.cse4780.trading;

import java.time.Duration;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;

/**
 * The Window API variant of {@code q1-ema} (D-42): Flink's window operator over 5-minute tumbling
 * event-time windows, with {@link CloseAggregate} and {@link EmaWindowFunction}, built as
 * {@code keyBy().window().sideOutputLateData().aggregate()} would build it.
 *
 * <p>It is constructed directly so that, like {@link EndOfInputProcessOperator}, it ends event
 * time itself when its input ends, whether or not the sources were idle then (D-38).
 */
public class EndOfInputWindowOperator
        extends WindowOperator<String, Tick, CloseCandidate, EmaResult, TimeWindow>
        implements BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** @param serializers the job's, as the window builder would use them */
    public EndOfInputWindowOperator(Variant.EmptyWindows emptyWindows, SerializerConfig serializers) {
        super(
                TumblingEventTimeWindows.of(Duration.ofMillis(EmaCalculator.WINDOW_MILLIS)),
                new TimeWindow.Serializer(),
                tick -> tick.symbol,
                StringSerializer.INSTANCE,
                new AggregatingStateDescriptor<>("window-contents", new CloseAggregate(),
                        TypeInformation.of(CloseCandidate.class).createSerializer(serializers)),
                new InternalSingleValueProcessWindowFunction<>(new EmaWindowFunction(emptyWindows)),
                EventTimeTrigger.create(),
                0,
                EmaCalculator.LATE_TICKS);
    }

    @Override
    public void endInput() throws Exception {
        processWatermark(Watermark.MAX_WATERMARK);
    }
}
