package fi.aalto.cse4780.trading;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A keyed process operator that fires its remaining event-time timers when its input ends.
 *
 * <p>Flink ends event time from the sources: at the end of a bounded input, and on
 * {@code flink stop --drain}, each source subtask sends the maximum watermark. A subtask that is
 * idle at that moment sends nothing, because its output drops watermarks while idle. If every
 * subtask is idle, no operator ever sees the end of event time, and the windows still open are
 * dropped unpublished and uncounted. A live replay drained after the idleness timeout lost its
 * last two windows that way (D-38).
 *
 * <p>End of input itself reaches this operator in both cases, so it ends event time there.
 */
public class EndOfInputProcessOperator<K, IN, OUT> extends KeyedProcessOperator<K, IN, OUT>
        implements BoundedOneInput {

    private static final long serialVersionUID = 1L;

    public EndOfInputProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
        super(function);
    }

    @Override
    public void endInput() throws Exception {
        processWatermark(Watermark.MAX_WATERMARK);
    }
}
