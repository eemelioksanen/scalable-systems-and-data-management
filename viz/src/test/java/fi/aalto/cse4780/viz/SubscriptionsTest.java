package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SubscriptionsTest {

    private final Subscriptions subs = new Subscriptions();

    @Test
    void theLatestListReplacesThePreviousOneAndATombstoneRemovesTheTrader() {
        assertTrue(subs.apply("t1", "{\"trader\":\"t1\",\"symbols\":[\"B.NL\",\"A.FR\",\"A.FR\"]}"));
        assertTrue(subs.apply("t2", "{\"symbols\":[\"*\"]}"));
        assertEquals(Map.of("t1", List.of("A.FR", "B.NL"), "t2", List.of("*")), subs.view().traders());

        assertTrue(subs.apply("t1", "{\"symbols\":[]}"));
        assertTrue(subs.apply("t2", null));
        assertEquals(Map.of("t1", List.of()), subs.view().traders());
    }

    @Test
    void rejectsWhatTheJobRejects() {
        assertFalse(subs.apply(null, "{\"symbols\":[]}"));
        assertFalse(subs.apply("t1", "{\"trader\":\"t2\",\"symbols\":[]}"));
        assertFalse(subs.apply("t1", "{\"symbols\":\"A.FR\"}"));
        assertFalse(subs.apply("t1", "{\"symbols\":[\"\"]}"));
        assertFalse(subs.apply("t1", "{\"symbols\":[1]}"));
        assertFalse(subs.apply("t1", "garbage"));
        assertEquals(0, subs.size());
    }

    @Test
    void theVersionMovesOnlyOnAChange() {
        long v0 = subs.view().version();
        subs.apply("t1", "{\"symbols\":[\"A.FR\"]}");
        long v1 = subs.view().version();
        subs.apply("t1", "{\"symbols\":[\"A.FR\"]}");
        subs.apply("t9", null);

        assertTrue(v1 > v0);
        assertEquals(v1, subs.view().version());
    }
}
