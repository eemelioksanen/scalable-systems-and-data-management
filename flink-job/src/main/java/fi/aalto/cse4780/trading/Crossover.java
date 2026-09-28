package fi.aalto.cse4780.trading;

/**
 * Query 2: a crossing of EMA38 and EMA100 between two consecutive evaluated windows of one symbol
 * (spec equations (1)-(4), docs/interfaces.md).
 */
public enum Crossover {

    /** EMA38 crosses above EMA100: previous EMA38 ≤ EMA100 and current EMA38 > EMA100. */
    BUY,

    /** EMA38 crosses below EMA100: previous EMA38 ≥ EMA100 and current EMA38 < EMA100. */
    SELL;

    /**
     * The crossover from the previous to the current window, or null if there is none.
     *
     * <p>The previous EMAs belong to the symbol's last window with a price event, since empty windows
     * are skipped (D-6). Before its first window they are both 0 (D-7), which satisfies the BUY
     * condition's {@code ≤}, so every symbol's first positive close is a BUY. Equal current EMAs are
     * on neither side, so they never produce an advisory.
     */
    public static Crossover detect(
            double previousEma38, double previousEma100, double ema38, double ema100) {
        if (previousEma38 <= previousEma100 && ema38 > ema100) {
            return BUY;
        }
        if (previousEma38 >= previousEma100 && ema38 < ema100) {
            return SELL;
        }
        return null;
    }
}
