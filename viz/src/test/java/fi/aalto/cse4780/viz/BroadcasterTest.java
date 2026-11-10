package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BroadcasterTest {

    static final ObjectMapper JSON = new ObjectMapper();

    static final class FakeClient implements Client {
        final List<JsonNode> sent = new ArrayList<>();
        long pending;
        String closed;

        @Override
        public void send(String text) {
            try {
                sent.add(JSON.readTree(text));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public long pendingChars() {
            return pending;
        }

        @Override
        public void close(String reason) {
            closed = reason;
        }

        List<String> types() {
            return sent.stream().map(m -> m.get("type").asText()).toList();
        }
    }

    private final Store store = new Store();
    private final FeedClock feed = new FeedClock(ZoneOffset.of("+02:00"));
    private final Subscriptions subscriptions = new Subscriptions();
    private final Broadcaster broadcaster =
            new Broadcaster(store, subscriptions, feed, ZoneOffset.of("+02:00"), () -> 5_000L);

    @Test
    void aNewSessionGetsASnapshotOnTheNextFlush() {
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();

        assertEquals(List.of("snapshot"), client.types());
        JsonNode snapshot = client.sent.get(0);
        assertEquals(StoreTest.T0, snapshot.get("spanEnd").asLong());
        assertEquals(288, snapshot.get("symbols").get(0).get("cells").asText().length());
        assertTrue(snapshot.get("series").isNull());
        assertEquals("+02:00", snapshot.get("clock").get("zoneOffset").asText());
        assertEquals(0, snapshot.get("advisories").size());
        assertEquals(0, snapshot.get("traders").size());
    }

    @Test
    void theSnapshotCarriesEveryAdvisoryAndTheTraders() {
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        store.addAdvisory(StoreTest.buy("A.FR", StoreTest.T0));
        subscriptions.apply("t1", "{\"symbols\":[\"A.FR\",\"B.NL\"]}");
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();

        JsonNode snapshot = client.sent.get(0);
        JsonNode a = snapshot.get("advisories").get(0);
        assertEquals("A.FR", a.get(0).asText());
        assertEquals(StoreTest.T0, a.get(1).asLong());
        assertEquals("BUY", a.get(2).asText());
        assertEquals("FR", a.get(4).asText());
        assertEquals(2, snapshot.get("traders").get("t1").size());
    }

    @Test
    void newAdvisoriesAndChangedTradersGoToEverySessionOnce() {
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();
        store.addAdvisory(StoreTest.buy("A.FR", StoreTest.T0));
        broadcaster.flush();
        subscriptions.apply("t1", "{\"symbols\":[\"A.FR\"]}");
        broadcaster.flush();
        subscriptions.apply("t1", "{\"symbols\":[\"A.FR\"]}");
        broadcaster.flush();

        assertEquals(List.of("snapshot", "delta", "delta"), client.types());
        JsonNode advisoryDelta = client.sent.get(1);
        assertEquals(1, advisoryDelta.get("advisories").size());
        assertTrue(advisoryDelta.get("traders").isNull());
        JsonNode tradersDelta = client.sent.get(2);
        assertEquals(0, tradersDelta.get("advisories").size());
        assertEquals("A.FR", tradersDelta.get("traders").get("t1").get(0).asText());
    }

    @Test
    void selectingASymbolSendsItsSeriesThenOnlyItsPoints() {
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();
        broadcaster.select(client, "A.FR");
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        store.add(StoreTest.up("B.NL", StoreTest.T0));
        broadcaster.flush();

        assertEquals(List.of("snapshot", "series", "delta"), client.types());
        JsonNode delta = client.sent.get(2);
        assertEquals(2, delta.get("cells").size());
        assertEquals("A.FR", delta.get("points").get("symbol").asText());
        assertEquals(1, delta.get("points").get("points").size());
    }

    @Test
    void anIdleSessionGetsNothing() {
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();
        broadcaster.flush();

        assertEquals(List.of("snapshot"), client.types());
    }

    @Test
    void eachChangeIsSentOnce() {
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        broadcaster.flush();
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        broadcaster.flush();
        broadcaster.flush();

        assertEquals(List.of("snapshot", "delta"), client.types());
    }

    @Test
    void aSessionTooFarBehindIsClosed() {
        FakeClient client = new FakeClient();
        broadcaster.connect(client);
        client.pending = Broadcaster.MAX_PENDING_CHARS + 1;
        broadcaster.flush();

        assertEquals("too far behind", client.closed);
        assertEquals(0, broadcaster.sessionCount());
        assertTrue(client.sent.isEmpty());
    }
}
