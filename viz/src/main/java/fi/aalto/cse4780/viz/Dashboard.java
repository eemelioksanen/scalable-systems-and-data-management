package fi.aalto.cse4780.viz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The dashboard backend: Kafka readers, the store, and the HTTP + WebSocket server. */
public final class Dashboard implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Store store;
    private final Broadcaster broadcaster;
    private final ResultsConsumer results;
    private final TicksTail ticks;
    private final Javalin app;
    private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "flush"));
    private final Map<String, WsClient> clients = new ConcurrentHashMap<>();

    Dashboard(DashboardOptions options, Store store, FeedClock feed, ResultsConsumer results, TicksTail ticks) {
        this.store = store;
        this.results = results;
        this.ticks = ticks;
        this.broadcaster = new Broadcaster(store, feed, options.zoneOffset(), System::currentTimeMillis);
        this.app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            if (Dashboard.class.getResource("/static/index.html") != null) {
                config.staticFiles.add("/static", Location.CLASSPATH);
            }
        });
        app.get("/health", ctx -> ctx.contentType("application/json").result(health().toString()));
        app.ws("/ws", ws -> {
            ws.onConnect(ctx -> {
                ctx.enableAutomaticPings(15, TimeUnit.SECONDS);
                WsClient client = new WsClient(ctx);
                clients.put(ctx.sessionId(), client);
                broadcaster.connect(client);
            });
            ws.onMessage(ctx -> {
                WsClient client = clients.get(ctx.sessionId());
                String symbol = selectedSymbol(ctx.message());
                if (client != null && symbol != null) {
                    broadcaster.select(client, symbol);
                }
            });
            ws.onClose(ctx -> drop(ctx.sessionId()));
            ws.onError(ctx -> drop(ctx.sessionId()));
        });
        startThread(results, "results");
        startThread(ticks, "ticks-tail");
        app.start(options.port());
        flusher.scheduleWithFixedDelay(this::flushSafely, options.flushMs(), options.flushMs(), TimeUnit.MILLISECONDS);
        LOG.info("Dashboard on http://localhost:{} reading {} and {} from {}", app.port(),
                options.resultsTopic(), options.ticksTopic(), options.bootstrapServers());
    }

    public static void main(String[] args) {
        DashboardOptions options;
        try {
            options = DashboardOptions.parse(args, System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        Store store = new Store();
        FeedClock feed = new FeedClock(options.zoneOffset());
        Dashboard dashboard = new Dashboard(options, store, feed,
                new ResultsConsumer(() -> consumer(options, "results"), options.resultsTopic(), store),
                new TicksTail(() -> consumer(options, "ticks"), options.ticksTopic(), feed, System::currentTimeMillis));
        Runtime.getRuntime().addShutdownHook(new Thread(dashboard::close));
    }

    int port() {
        return app.port();
    }

    ObjectNode health() {
        Messages.Clock clock = broadcaster.clock();
        ObjectNode o = JSON.createObjectNode();
        o.put("resultsRead", results.resultsRead());
        o.put("malformedResults", results.malformedResults());
        o.put("staleResults", store.staleResults());
        o.put("symbols", store.symbolCount());
        o.put("sessions", broadcaster.sessionCount());
        putLong(o, "feedMs", clock.feedMs());
        putLong(o, "resultsMs", clock.resultsMs());
        return o;
    }

    @Override
    public void close() {
        flusher.shutdownNow();
        results.stop();
        ticks.stop();
        app.stop();
    }

    static String selectedSymbol(String message) {
        try {
            JsonNode node = JSON.readTree(message);
            if (node != null && "select".equals(node.path("type").asText()) && node.path("symbol").isTextual()) {
                return node.get("symbol").asText();
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    private void flushSafely() {
        try {
            broadcaster.flush();
        } catch (RuntimeException e) {
            LOG.warn("Flushing to browsers failed", e);
        }
    }

    private void drop(String sessionId) {
        WsClient client = clients.remove(sessionId);
        if (client != null) {
            broadcaster.disconnect(client);
        }
    }

    private static KafkaConsumer<String, String> consumer(DashboardOptions options, String name) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "viz-" + name);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }

    private static void startThread(Runnable r, String name) {
        daemon(r, name).start();
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, "viz-" + name);
        t.setDaemon(true);
        return t;
    }

    private static void putLong(ObjectNode o, String field, Long value) {
        if (value == null) {
            o.putNull(field);
        } else {
            o.put(field, value.longValue());
        }
    }

    /** Sends without blocking the flush thread, counting what Jetty has not written yet. */
    private static final class WsClient implements Client {
        private final WsContext ctx;
        private final AtomicLong pending = new AtomicLong();

        WsClient(WsContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void send(String text) {
            long n = text.length();
            pending.addAndGet(n);
            try {
                ctx.session.getRemote().sendString(text, new WriteCallback() {
                    @Override
                    public void writeFailed(Throwable x) {
                        pending.addAndGet(-n);
                    }

                    @Override
                    public void writeSuccess() {
                        pending.addAndGet(-n);
                    }
                });
            } catch (RuntimeException e) {
                pending.addAndGet(-n);
                LOG.debug("Send to {} failed", ctx.sessionId(), e);
            }
        }

        @Override
        public long pendingChars() {
            return pending.get();
        }

        @Override
        public void close(String reason) {
            try {
                ctx.closeSession(1013, reason);
            } catch (RuntimeException e) {
                LOG.debug("Closing {} failed", ctx.sessionId(), e);
            }
        }
    }
}
