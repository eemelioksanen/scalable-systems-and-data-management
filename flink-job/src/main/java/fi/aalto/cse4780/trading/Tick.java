package fi.aalto.cse4780.trading;

/**
 * A price event: a {@code ticks} record whose {@code Last} field is set (D-2).
 *
 * <p>Public fields and a no-arg constructor let Flink use its POJO serializer.
 */
public class Tick {

    /** Symbol key, e.g. {@code RDSA.NL} (D-1). */
    public String symbol;

    /** Security type: {@code E} (equity) or {@code I} (index). */
    public String secType;

    /** The {@code Last} trade price. */
    public double price;

    /**
     * {@code Trading date} + {@code Trading time} as epoch nanoseconds (D-3). Kept at the source's
     * sub-millisecond precision so the close is the latest trade even within one millisecond (D-4).
     */
    public long eventTimeNanos;

    /**
     * Arrival order within the symbol's Kafka partition, i.e. the record offset (D-15). Breaks ties
     * between equal event times (D-4).
     */
    public long sourceOrder;

    /** Wall-clock time (epoch ms) when the replayer sent the record: the Kafka record timestamp. */
    public long ingestTimeMillis;

    public Tick() {}

    public Tick(
            String symbol,
            String secType,
            double price,
            long eventTimeNanos,
            long sourceOrder,
            long ingestTimeMillis) {
        this.symbol = symbol;
        this.secType = secType;
        this.price = price;
        this.eventTimeNanos = eventTimeNanos;
        this.sourceOrder = sourceOrder;
        this.ingestTimeMillis = ingestTimeMillis;
    }

    /** Event time in epoch milliseconds, used for Flink timestamps, watermarks and windows. */
    public long eventTimeMillis() {
        return Math.floorDiv(eventTimeNanos, 1_000_000L);
    }

    @Override
    public String toString() {
        return "Tick{" + symbol + ' ' + secType + " price=" + price + " eventTimeNanos=" + eventTimeNanos
                + " sourceOrder=" + sourceOrder + " ingestTimeMillis=" + ingestTimeMillis + '}';
    }
}
