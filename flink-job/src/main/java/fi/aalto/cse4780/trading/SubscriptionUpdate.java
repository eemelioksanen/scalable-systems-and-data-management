package fi.aalto.cse4780.trading;

import java.util.Arrays;

/**
 * One {@code subscriptions} record (D-17) and its position in the topic.
 *
 * <p>Public fields and a no-arg constructor let Flink use its POJO serializer.
 */
public class SubscriptionUpdate {

    /** The record key. Null when the record is malformed: it changes nothing, only its position counts. */
    public String trader;

    /**
     * The trader's complete symbol list, which replaces the previous one. {@code *} means every
     * symbol, and an empty list means none. Null for a tombstone, which removes the trader.
     */
    public String[] symbols;

    public int partition;
    public long offset;

    public SubscriptionUpdate() {}

    public SubscriptionUpdate(String trader, String[] symbols, int partition, long offset) {
        this.trader = trader;
        this.symbols = symbols;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "SubscriptionUpdate{" + trader + ' ' + Arrays.toString(symbols) + " partition="
                + partition + " offset=" + offset + '}';
    }
}
