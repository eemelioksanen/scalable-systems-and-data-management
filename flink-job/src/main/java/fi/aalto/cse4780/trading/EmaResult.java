package fi.aalto.cse4780.trading;

/** One Query 1 result, published to {@code ema-results} (D-16, docs/interfaces.md). */
public class EmaResult {

    public String symbol;
    public String secType;

    /** Exclusive end of the 5-minute window, epoch ms. */
    public long windowEnd;

    public double close;
    public double ema38;
    public double ema100;

    /** Latest ingest time (epoch ms) among the window's price events. */
    public long maxIngestTs;

    /** Wall-clock time (epoch ms) when the result was emitted. */
    public long emitTs;

    public EmaResult() {}

    public EmaResult(
            String symbol,
            String secType,
            long windowEnd,
            double close,
            double ema38,
            double ema100,
            long maxIngestTs,
            long emitTs) {
        this.symbol = symbol;
        this.secType = secType;
        this.windowEnd = windowEnd;
        this.close = close;
        this.ema38 = ema38;
        this.ema100 = ema100;
        this.maxIngestTs = maxIngestTs;
        this.emitTs = emitTs;
    }

    @Override
    public String toString() {
        return "EmaResult{" + symbol + ' ' + secType + " windowEnd=" + windowEnd + " close=" + close
                + " ema38=" + ema38 + " ema100=" + ema100 + " maxIngestTs=" + maxIngestTs
                + " emitTs=" + emitTs + '}';
    }
}
