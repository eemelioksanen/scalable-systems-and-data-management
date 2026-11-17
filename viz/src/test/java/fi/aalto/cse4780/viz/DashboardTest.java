package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import static fi.aalto.cse4780.viz.KafkaFixtures.supply;

import org.junit.jupiter.api.Test;

class DashboardTest {

    static final ObjectMapper JSON = new ObjectMapper();

    private Store store;
    private Subscriptions subscriptions;
    private Dashboard dashboard;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() {
        store = new Store();
        subscriptions = new Subscriptions();
        DashboardOptions options = DashboardOptions.parse(new String[] {"--port", "0", "--flush-ms", "20"}, Map.of());
        FeedClock feed = new FeedClock(ZoneOffset.of("+02:00"));
        dashboard = new Dashboard(options, store, subscriptions, feed,
                TopicReader.results(supply(KafkaFixtures.consumerWithTopic("ema-results", 1)), "ema-results", store),
                TopicReader.advisories(supply(KafkaFixtures.consumerWithTopic("advisories", 1)), "advisories", store),
                TopicReader.subscriptions(supply(KafkaFixtures.consumerWithTopic("subscriptions", 1)), "subscriptions",
                        subscriptions),
                new TicksTail(supply(KafkaFixtures.consumerWithTopic("ticks", 1)), "ticks", feed, System::currentTimeMillis));
    }

    @AfterEach
    void stop() {
        dashboard.close();
    }

    /** Collects the text messages one browser receives. */
    static final class Browser implements WebSocket.Listener {
        final BlockingQueue<JsonNode> messages = new LinkedBlockingQueue<>();
        private final StringBuilder partial = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                try {
                    messages.add(JSON.readTree(partial.toString()));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                partial.setLength(0);
            }
            ws.request(1);
            return null;
        }

        JsonNode next(String type) throws InterruptedException {
            while (true) {
                JsonNode m = messages.poll(5, TimeUnit.SECONDS);
                if (m == null) {
                    throw new AssertionError("no " + type + " message");
                }
                if (m.get("type").asText().equals(type)) {
                    return m;
                }
            }
        }
    }

    private WebSocket connect(Browser browser) {
        return http.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + dashboard.port() + "/ws"), browser)
                .join();
    }

    @Test
    void connectSelectAndReceiveUpdates() throws Exception {
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        Browser browser = new Browser();
        WebSocket ws = connect(browser);

        JsonNode snapshot = browser.next("snapshot");
        assertEquals("A.FR", snapshot.get("symbols").get(0).get("symbol").asText());

        ws.sendText("{\"type\":\"select\",\"symbol\":\"A.FR\"}", true).join();
        JsonNode series = browser.next("series");
        assertEquals(1, series.get("series").get("points").size());
        assertEquals(StoreTest.T0, series.get("series").get("firstWindowEnd").asLong());

        store.add(StoreTest.down("A.FR", StoreTest.w(1)));
        store.addAdvisory(StoreTest.sell("A.FR", StoreTest.w(1)));
        JsonNode delta = browser.next("delta");
        assertEquals("-", delta.get("cells").get(0).get(2).asText());
        JsonNode cell = delta.get("cells").get(0);
        assertEquals(List.of(10.0, 1.0, 2.0), List.of(cell.get(6).asDouble(), cell.get(7).asDouble(), cell.get(8).asDouble()));
        assertEquals(StoreTest.w(1), delta.get("points").get("points").get(0).get(0).asLong());
        assertEquals("SELL", delta.get("advisories").get(0).get(2).asText());

        subscriptions.apply("t1", "{\"symbols\":[\"A.FR\"]}");
        JsonNode traders = browser.next("delta");
        assertEquals("A.FR", traders.get("traders").get("t1").get(0).asText());
        ws.abort();
    }

    @Test
    void aReconnectStartsFromAFreshSnapshot() throws Exception {
        Browser first = new Browser();
        WebSocket ws = connect(first);
        first.next("snapshot");
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();

        store.add(StoreTest.up("A.FR", StoreTest.T0));
        Browser second = new Browser();
        WebSocket ws2 = connect(second);
        JsonNode snapshot = second.next("snapshot");
        assertEquals(1, snapshot.get("symbols").size());
        ws2.abort();
    }

    @Test
    void healthReportsCounts() throws Exception {
        store.add(StoreTest.up("A.FR", StoreTest.T0));
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + dashboard.port() + "/health")).build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode health = JSON.readTree(response.body());
        assertEquals(200, response.statusCode());
        assertEquals(1, health.get("symbols").asInt());
        assertEquals(0, health.get("advisories").asInt());
        assertEquals(0, health.get("traders").asInt());
        assertEquals(StoreTest.T0, health.get("resultsMs").asLong());
        assertTrue(health.get("feedMs").isNull());
    }

    @Test
    void parsesSelectMessagesOnly() {
        assertEquals("A.FR", Dashboard.selectedSymbol("{\"type\":\"select\",\"symbol\":\"A.FR\"}"));
        assertEquals(null, Dashboard.selectedSymbol("{\"type\":\"other\",\"symbol\":\"A.FR\"}"));
        assertEquals(null, Dashboard.selectedSymbol("nonsense"));
    }

}
