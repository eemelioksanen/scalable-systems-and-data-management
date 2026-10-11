package fi.aalto.cse4780.viz;

import static fi.aalto.cse4780.viz.Store.SLOTS;
import static fi.aalto.cse4780.viz.Store.SLOT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StoreTest {

    /** The window ending 09:05 on 08-11-2021 at +02:00 (docs/interfaces.md). */
    static final long T0 = 1_636_355_100_000L;

    private final Store store = new Store();

    static Result up(String symbol, long windowEnd) {
        return new Result(symbol, "E", windowEnd, 10.0, 2.0, 1.0);
    }

    static Result down(String symbol, long windowEnd) {
        return new Result(symbol, "E", windowEnd, 10.0, 1.0, 2.0);
    }

    static long w(int slot) {
        return T0 + slot * SLOT_MS;
    }

    @Test
    void keepsWindowsSortedAndReplacesADuplicateInPlace() {
        store.add(up("RDSA.NL", w(2)));
        store.add(up("RDSA.NL", w(0)));
        store.add(down("RDSA.NL", w(1)));
        store.add(new Result("RDSA.NL", "E", w(1), 11.5, 1.0, 2.0));

        List<Store.Point> points = store.series("RDSA.NL").points();
        assertEquals(List.of(w(0), w(1), w(2)), points.stream().map(Store.Point::windowEnd).toList());
        assertEquals(11.5, points.get(1).close());
    }

    @Test
    void evictsWindowsThatLeaveTheSpanWhenItAdvances() {
        store.add(up("A.FR", w(0)));
        store.add(up("A.FR", w(1)));
        store.add(up("B.NL", w(SLOTS)));

        assertEquals(List.of(w(1)), store.series("A.FR").points().stream().map(Store.Point::windowEnd).toList());
        assertEquals(w(SLOTS), store.latestWindowEnd());
    }

    @Test
    void dropsAndCountsResultsOlderThanTheSpan() {
        store.add(up("A.FR", w(SLOTS)));

        assertFalse(store.add(up("A.FR", w(0))));
        assertTrue(store.add(up("A.FR", w(1))));
        assertEquals(1, store.staleResults());
    }

    @Test
    void retainsAFullDayOfWindowsPerSymbol() {
        for (int i = 0; i < SLOTS * 3; i++) {
            store.add(up("A.FR", w(i)));
        }
        List<Store.Point> points = store.series("A.FR").points();
        assertEquals(SLOTS, points.size());
        assertEquals(w(SLOTS * 2), points.get(0).windowEnd());
    }

    @Test
    void rowCellsAreOldestFirstWithDotsForMissingWindows() {
        store.add(up("A.FR", w(SLOTS - 1)));
        store.add(down("A.FR", w(SLOTS - 3)));
        store.add(new Result("A.FR", "E", w(0), 1.0, 1.0, 1.0));

        String cells = store.rows().get(0).cells();
        assertEquals(SLOTS, cells.length());
        assertEquals('0', cells.charAt(0));
        assertEquals('-', cells.charAt(SLOTS - 3));
        assertEquals('.', cells.charAt(SLOTS - 2));
        assertEquals('+', cells.charAt(SLOTS - 1));
    }

    @Test
    void theFirstWindowIsAFlipFromZero() {
        store.add(up("A.FR", w(0)));
        store.add(up("A.FR", w(1)));

        assertEquals(w(0), store.rows().get(0).lastFlip());
        assertEquals(w(0), store.series("A.FR").firstWindowEnd());
    }

    @Test
    void lastFlipIsTheLatestSignChangeIncludingEqualEmas() {
        store.add(up("A.FR", w(0)));
        store.add(down("A.FR", w(1)));
        store.add(down("A.FR", w(2)));
        assertEquals(w(1), store.rows().get(0).lastFlip());

        store.add(new Result("A.FR", "E", w(3), 1.0, 1.5, 1.5));
        assertEquals(w(3), store.rows().get(0).lastFlip());
    }

    @Test
    void lastFlipIsNullOnceTheFlipHasLeftTheSpan() {
        store.add(down("A.FR", w(0)));
        store.add(down("A.FR", w(1)));
        store.add(down("A.FR", w(SLOTS)));

        assertNull(store.rows().get(0).lastFlip());
    }

    @Test
    void drainReportsEachChangeOnceAndPointsOnlyForRequestedSymbols() {
        store.add(up("A.FR", w(0)));
        store.add(down("B.NL", w(0)));
        store.add(down("A.FR", w(0)));

        Store.Drained first = store.drain(Set.of("A.FR"));
        assertEquals(2, first.cells().size());
        Store.Cell a = first.cells().stream().filter(c -> c.symbol().equals("A.FR")).findFirst().orElseThrow();
        assertEquals('-', a.sign());
        assertEquals("FR", a.exchange());
        assertEquals(List.of(w(0)), first.points().get("A.FR").stream().map(Store.Point::windowEnd).toList());
        assertFalse(first.points().containsKey("B.NL"));

        Store.Drained second = store.drain(Set.of("A.FR"));
        assertTrue(second.cells().isEmpty());
        assertEquals(w(0), second.spanEnd());
    }

    @Test
    void drainSkipsChangesThatLeftTheSpanBeforeTheyWereSent() {
        store.add(up("A.FR", w(0)));
        store.add(up("B.NL", w(SLOTS)));

        Store.Drained drained = store.drain(Set.of());
        assertEquals(List.of("B.NL"), drained.cells().stream().map(Store.Cell::symbol).toList());
    }

    @Test
    void anUnknownSymbolHasAnEmptySeries() {
        Store.Series series = store.series("NOPE.FR");
        assertTrue(series.points().isEmpty());
        assertNull(series.firstWindowEnd());
        assertNull(store.latestWindowEnd());
    }
}
