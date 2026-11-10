package fi.aalto.cse4780.viz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The last {@value #SLOTS} five-minute windows of every published symbol, ending at the newest
 * {@code windowEnd} seen. Memory is bounded by symbols × {@value #SLOTS} however long a replay runs.
 */
final class Store {

    static final int SLOTS = 288;
    static final long SLOT_MS = 300_000L;

    record Point(long windowEnd, double close, double ema38, double ema100) {}

    record Cell(String symbol, long windowEnd, char sign, String secType, String exchange, Long lastFlip) {}

    record Row(String symbol, String secType, String exchange, Long lastFlip, String cells) {}

    record Series(String symbol, Long firstWindowEnd, List<Point> points) {}

    /**
     * Changes since the previous drain: heatmap cells and advisories for everyone, points for the
     * symbols asked for.
     */
    record Drained(Long spanEnd, List<Cell> cells, Map<String, List<Point>> points, List<Advisory> advisories) {}

    private final Map<String, History> symbols = new HashMap<>();
    private final Map<String, Set<Long>> pending = new LinkedHashMap<>();
    /** Advisories by symbol and windowEnd, within the span. Not tied to History: the two topics are read apart. */
    private final Map<String, TreeMap<Long, Advisory>> advisories = new HashMap<>();
    private final Map<String, Set<Long>> pendingAdvisories = new LinkedHashMap<>();
    private int advisoryCount;
    private long latestWindowEnd = Long.MIN_VALUE;
    private long staleResults;
    private long staleAdvisories;

    /** Adds a result. Returns false if it is older than the retained span and was dropped. */
    synchronized boolean add(Result r) {
        if (latestWindowEnd != Long.MIN_VALUE && r.windowEnd() <= spanStart(latestWindowEnd)) {
            staleResults++;
            return false;
        }
        if (r.windowEnd() > latestWindowEnd) {
            latestWindowEnd = r.windowEnd();
            // Also on the first result: advisories read before it may already be out of the span.
            evictBefore(spanStart(latestWindowEnd));
        }
        History h = symbols.computeIfAbsent(r.symbol(), s -> new History(r.secType(), Result.exchangeOf(s)));
        h.secType = r.secType();
        h.put(r);
        pending.computeIfAbsent(r.symbol(), s -> new LinkedHashSet<>()).add(r.windowEnd());
        return true;
    }

    /**
     * Adds an advisory. Returns false if it is older than the retained span and was dropped. The
     * span follows {@code ema-results} only, so an advisory read before its result is kept.
     */
    synchronized boolean addAdvisory(Advisory a) {
        if (latestWindowEnd != Long.MIN_VALUE && a.windowEnd() <= spanStart(latestWindowEnd)) {
            staleAdvisories++;
            return false;
        }
        if (advisories.computeIfAbsent(a.symbol(), s -> new TreeMap<>()).put(a.windowEnd(), a) == null) {
            advisoryCount++;
        }
        pendingAdvisories.computeIfAbsent(a.symbol(), s -> new LinkedHashSet<>()).add(a.windowEnd());
        return true;
    }

    /** Every retained advisory, oldest first, then by symbol. */
    synchronized List<Advisory> advisories() {
        List<Advisory> out = new ArrayList<>(advisoryCount);
        for (TreeMap<Long, Advisory> bySymbol : advisories.values()) {
            out.addAll(bySymbol.values());
        }
        out.sort(ADVISORY_ORDER);
        return out;
    }

    synchronized int advisoryCount() {
        return advisoryCount;
    }

    synchronized long staleAdvisories() {
        return staleAdvisories;
    }

    synchronized Long latestWindowEnd() {
        return latestWindowEnd == Long.MIN_VALUE ? null : latestWindowEnd;
    }

    synchronized long staleResults() {
        return staleResults;
    }

    synchronized int symbolCount() {
        return symbols.size();
    }

    /** Every symbol's heatmap row: {@value #SLOTS} characters, oldest first. */
    synchronized List<Row> rows() {
        List<Row> rows = new ArrayList<>(symbols.size());
        for (Map.Entry<String, History> e : symbols.entrySet()) {
            History h = e.getValue();
            rows.add(new Row(e.getKey(), h.secType, h.exchange, h.lastFlip(), h.cells(latestWindowEnd)));
        }
        return rows;
    }

    /** The retained windows of one symbol, oldest first; empty points if it has none. */
    synchronized Series series(String symbol) {
        History h = symbols.get(symbol);
        if (h == null) {
            return new Series(symbol, null, List.of());
        }
        return new Series(symbol, h.firstWindowEnd, h.points(0, h.size));
    }

    /** Takes every change since the last call. Windows that have left the span are not reported. */
    synchronized Drained drain(Set<String> pointsFor) {
        List<Cell> cells = new ArrayList<>();
        Map<String, List<Point>> points = new HashMap<>();
        for (Map.Entry<String, Set<Long>> e : pending.entrySet()) {
            History h = symbols.get(e.getKey());
            boolean wanted = pointsFor.contains(e.getKey());
            Long lastFlip = h.lastFlip();
            for (long windowEnd : e.getValue()) {
                int i = h.indexOf(windowEnd);
                if (i < 0) {
                    continue;
                }
                cells.add(new Cell(e.getKey(), windowEnd, signChar(h.sign[i]), h.secType, h.exchange, lastFlip));
                if (wanted) {
                    points.computeIfAbsent(e.getKey(), s -> new ArrayList<>()).add(h.point(i));
                }
            }
        }
        pending.clear();
        List<Advisory> newAdvisories = new ArrayList<>();
        for (Map.Entry<String, Set<Long>> e : pendingAdvisories.entrySet()) {
            TreeMap<Long, Advisory> bySymbol = advisories.get(e.getKey());
            for (long windowEnd : e.getValue()) {
                Advisory a = bySymbol == null ? null : bySymbol.get(windowEnd);
                if (a != null) {
                    newAdvisories.add(a);
                }
            }
        }
        pendingAdvisories.clear();
        newAdvisories.sort(ADVISORY_ORDER);
        return new Drained(latestWindowEnd(), cells, points, newAdvisories);
    }

    private static final java.util.Comparator<Advisory> ADVISORY_ORDER =
            java.util.Comparator.comparingLong(Advisory::windowEnd).thenComparing(Advisory::symbol);

    static long spanStart(long spanEnd) {
        return spanEnd - SLOTS * SLOT_MS;
    }

    static char signChar(byte sign) {
        return sign > 0 ? '+' : sign < 0 ? '-' : '0';
    }

    private void evictBefore(long spanStartExclusive) {
        for (History h : symbols.values()) {
            h.evictUpTo(spanStartExclusive);
        }
        for (TreeMap<Long, Advisory> bySymbol : advisories.values()) {
            var old = bySymbol.headMap(spanStartExclusive, true);
            advisoryCount -= old.size();
            old.clear();
        }
        advisories.values().removeIf(TreeMap::isEmpty);
    }

    /** One symbol's retained windows in parallel arrays, sorted by windowEnd. */
    private static final class History {
        String secType;
        final String exchange;
        Long firstWindowEnd;
        final long[] windowEnd = new long[SLOTS];
        final double[] close = new double[SLOTS];
        final double[] ema38 = new double[SLOTS];
        final double[] ema100 = new double[SLOTS];
        final byte[] sign = new byte[SLOTS];
        int size;

        History(String secType, String exchange) {
            this.secType = secType;
            this.exchange = exchange;
        }

        void put(Result r) {
            if (firstWindowEnd == null || r.windowEnd() < firstWindowEnd) {
                firstWindowEnd = r.windowEnd();
            }
            int i = Arrays.binarySearch(windowEnd, 0, size, r.windowEnd());
            if (i < 0) {
                i = -i - 1;
                if (size == SLOTS) {
                    // Every slot of the span is held, so a new in-span window cannot exist; keep the newest.
                    if (i == 0) {
                        return;
                    }
                    shiftLeft(1);
                    i--;
                }
                System.arraycopy(windowEnd, i, windowEnd, i + 1, size - i);
                System.arraycopy(close, i, close, i + 1, size - i);
                System.arraycopy(ema38, i, ema38, i + 1, size - i);
                System.arraycopy(ema100, i, ema100, i + 1, size - i);
                System.arraycopy(sign, i, sign, i + 1, size - i);
                size++;
            }
            windowEnd[i] = r.windowEnd();
            close[i] = r.close();
            ema38[i] = r.ema38();
            ema100[i] = r.ema100();
            sign[i] = (byte) Double.compare(r.ema38(), r.ema100());
        }

        void evictUpTo(long spanStartExclusive) {
            int keepFrom = 0;
            while (keepFrom < size && windowEnd[keepFrom] <= spanStartExclusive) {
                keepFrom++;
            }
            if (keepFrom > 0) {
                shiftLeft(keepFrom);
            }
        }

        private void shiftLeft(int n) {
            int remaining = size - n;
            System.arraycopy(windowEnd, n, windowEnd, 0, remaining);
            System.arraycopy(close, n, close, 0, remaining);
            System.arraycopy(ema38, n, ema38, 0, remaining);
            System.arraycopy(ema100, n, ema100, 0, remaining);
            System.arraycopy(sign, n, sign, 0, remaining);
            size = remaining;
        }

        int indexOf(long end) {
            int i = Arrays.binarySearch(windowEnd, 0, size, end);
            return i < 0 ? -1 : i;
        }

        /**
         * The latest retained window whose sign differs from the window before it. The first window
         * ever seen counts as a flip from 0, since both EMAs start at 0 (D-7).
         */
        Long lastFlip() {
            for (int i = size - 1; i >= 0; i--) {
                byte previous;
                if (i > 0) {
                    previous = sign[i - 1];
                } else if (firstWindowEnd != null && windowEnd[0] == firstWindowEnd) {
                    previous = 0;
                } else {
                    return null;
                }
                if (sign[i] != previous) {
                    return windowEnd[i];
                }
            }
            return null;
        }

        String cells(long spanEnd) {
            char[] out = new char[SLOTS];
            Arrays.fill(out, '.');
            for (int i = 0; i < size; i++) {
                long column = SLOTS - 1 - (spanEnd - windowEnd[i]) / SLOT_MS;
                if (column >= 0 && column < SLOTS) {
                    out[(int) column] = signChar(sign[i]);
                }
            }
            return new String(out);
        }

        Point point(int i) {
            return new Point(windowEnd[i], close[i], ema38[i], ema100[i]);
        }

        List<Point> points(int from, int to) {
            List<Point> out = new ArrayList<>(to - from);
            for (int i = from; i < to; i++) {
                out.add(point(i));
            }
            return out;
        }
    }
}
