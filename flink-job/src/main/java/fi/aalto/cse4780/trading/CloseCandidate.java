package fi.aalto.cse4780.trading;

/**
 * The best close seen so far for one symbol and one still-open window: the price event with the
 * latest event time, ties going to the later source position (D-4, D-23).
 */
public class CloseCandidate {

    public double price;
    public long eventTimeMillis;
    public long sourceOrder;
    public String secType;

    /** Latest ingest time among all of the window's price events, not only the close. */
    public long maxIngestTimeMillis;

    public CloseCandidate() {}

    public CloseCandidate(Tick first) {
        price = first.price;
        eventTimeMillis = first.eventTimeMillis;
        sourceOrder = first.sourceOrder;
        secType = first.secType;
        maxIngestTimeMillis = first.ingestTimeMillis;
    }

    /** Takes {@code tick} as the close if it is later by event time, or equal and arrived later. */
    public void offer(Tick tick) {
        maxIngestTimeMillis = Math.max(maxIngestTimeMillis, tick.ingestTimeMillis);
        boolean later =
                tick.eventTimeMillis > eventTimeMillis
                        || (tick.eventTimeMillis == eventTimeMillis && tick.sourceOrder > sourceOrder);
        if (later) {
            price = tick.price;
            eventTimeMillis = tick.eventTimeMillis;
            sourceOrder = tick.sourceOrder;
            secType = tick.secType;
        }
    }
}
