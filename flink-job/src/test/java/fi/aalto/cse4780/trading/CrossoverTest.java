package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** The inequalities of spec equations (1)-(4), including every equality case. */
class CrossoverTest {

    @Test
    void firstPositiveCloseIsABuyFromZeroInitialEmas() {
        assertEquals(Crossover.BUY, Crossover.detect(0, 0, 100.0 * 2 / 39, 100.0 * 2 / 101));
    }

    @Test
    void crossingAboveIsABuy() {
        assertEquals(Crossover.BUY, Crossover.detect(9, 10, 11, 10));
    }

    @Test
    void crossingBelowIsASell() {
        assertEquals(Crossover.SELL, Crossover.detect(11, 10, 9, 10));
    }

    @Test
    void equalPreviousEmasCountForEitherDirection() {
        assertEquals(Crossover.BUY, Crossover.detect(10, 10, 11, 10));
        assertEquals(Crossover.SELL, Crossover.detect(10, 10, 9, 10));
    }

    @Test
    void equalCurrentEmasAreNoCrossover() {
        assertNull(Crossover.detect(9, 10, 10, 10));
        assertNull(Crossover.detect(11, 10, 10, 10));
        assertNull(Crossover.detect(10, 10, 10, 10));
    }

    @Test
    void staysSilentWhileTheEmasStayOnOneSide() {
        assertNull(Crossover.detect(11, 10, 12, 10));
        assertNull(Crossover.detect(9, 10, 8, 10));
    }
}
