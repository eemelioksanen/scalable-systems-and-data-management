package fi.aalto.cse4780.trading;

/** A symbol's EMAs after its most recently evaluated window. Both start at 0 (D-7). */
public class EmaState {

    public double ema38;
    public double ema100;

    public EmaState() {}
}
