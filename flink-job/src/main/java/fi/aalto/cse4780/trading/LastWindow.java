package fi.aalto.cse4780.trading;

/** A symbol's most recently evaluated window, kept only to carry its close forward (D-42). */
public class LastWindow {

    public long windowEnd;
    public double close;
    public String secType;

    public LastWindow() {}

    public LastWindow(long windowEnd, double close, String secType) {
        this.windowEnd = windowEnd;
        this.close = close;
        this.secType = secType;
    }
}
