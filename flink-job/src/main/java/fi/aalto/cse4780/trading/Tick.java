package fi.aalto.cse4780.trading;

/**
 * A price event: a {@code ticks} record whose {@code Last} is a positive price (D-2).
 *
 * <p>Public fields and a no-arg constructor let Flink use its POJO serializer.
 */
public class Tick {

    /** Symbol key, e.g. {@code RDSA.NL} (D-1). May be fully numeric, e.g. {@code 846741.ETR}. */
    public String symbol;

    /** Security type: {@code E} (equity) or {@code I} (index). */
    public String secType;

    /** The {@code Last} trade price, always > 0. */
    public double price;

    /**
     * Trading timestamp in epoch ms (D-3, D-25). The source has millisecond resolution, so this
     * loses nothing (D-22). Equal times are ordered by {@link #sourceOrder} (D-23).
     */
    public long eventTimeMillis;

    /**
     * Arrival order within the symbol's Kafka partition, i.e. the record offset (D-15, D-23). Breaks
     * ties between equal event times (D-4).
     */
    public long sourceOrder;

    /** Wall-clock time (epoch ms) when the replayer sent the record: the Kafka record timestamp. */
    public long ingestTimeMillis;

    public Integer sourceFileIndex;

    public Long sourceLine;

    public Tick() {}

    public Tick(
            String symbol,
            String secType,
            double price,
            long eventTimeMillis,
            long sourceOrder,
            long ingestTimeMillis) {
        this.symbol = symbol;
        this.secType = secType;
        this.price = price;
        this.eventTimeMillis = eventTimeMillis;
        this.sourceOrder = sourceOrder;
        this.ingestTimeMillis = ingestTimeMillis;
    }

    @Override
    public String toString() {
        return "Tick{" + symbol + ' ' + secType + " price=" + price + " eventTimeMillis="
                + eventTimeMillis + " sourceOrder=" + sourceOrder + " ingestTimeMillis="
                + ingestTimeMillis + '}';
    }
}
