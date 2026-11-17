package fi.aalto.cse4780.trading;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * The Window API variant's close selection (D-42): the same {@link CloseCandidate#offer} rule as
 * {@link EmaCalculator}, applied by Flink's window operator. An empty accumulator loses to any
 * tick.
 */
public class CloseAggregate implements AggregateFunction<Tick, CloseCandidate, CloseCandidate> {

    private static final long serialVersionUID = 1L;

    @Override
    public CloseCandidate createAccumulator() {
        CloseCandidate empty = new CloseCandidate();
        empty.eventTimeMillis = Long.MIN_VALUE;
        empty.maxIngestTimeMillis = Long.MIN_VALUE;
        return empty;
    }

    @Override
    public CloseCandidate add(Tick tick, CloseCandidate accumulator) {
        accumulator.offer(tick);
        return accumulator;
    }

    @Override
    public CloseCandidate getResult(CloseCandidate accumulator) {
        return accumulator;
    }

    @Override
    public CloseCandidate merge(CloseCandidate a, CloseCandidate b) {
        throw new UnsupportedOperationException("tumbling windows never merge");
    }
}
