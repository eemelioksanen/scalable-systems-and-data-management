package fi.aalto.cse4780.viz;

import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Pushes the {@link Store} to browsers. Every send happens in {@link #flush}, on one thread, so a
 * connect or a selection only marks the session and the next flush answers it.
 */
final class Broadcaster {

    /** A browser this far behind is disconnected; it reconnects to a fresh snapshot. */
    static final long MAX_PENDING_CHARS = 4L * 1024 * 1024;

    private static final class Session {
        volatile String selected;
        volatile boolean needsSnapshot = true;
        volatile boolean needsSeries;
        Long sentFeedMs;
        Long sentIdleSince;
        Long sentResultsMs;
    }

    private final Store store;
    private final FeedClock feed;
    private final String zoneOffset;
    private final LongSupplier wallClock;
    private final Map<Client, Session> sessions = new ConcurrentHashMap<>();

    Broadcaster(Store store, FeedClock feed, ZoneOffset zoneOffset, LongSupplier wallClock) {
        this.store = store;
        this.feed = feed;
        this.zoneOffset = zoneOffset.getId().equals("Z") ? "+00:00" : zoneOffset.getId();
        this.wallClock = wallClock;
    }

    void connect(Client client) {
        sessions.put(client, new Session());
    }

    void select(Client client, String symbol) {
        Session s = sessions.get(client);
        if (s != null && symbol != null && !symbol.isEmpty()) {
            s.selected = symbol;
            s.needsSeries = true;
        }
    }

    void disconnect(Client client) {
        sessions.remove(client);
    }

    int sessionCount() {
        return sessions.size();
    }

    synchronized void flush() {
        Set<String> selected = new HashSet<>();
        for (Session s : sessions.values()) {
            if (s.selected != null) {
                selected.add(s.selected);
            }
        }
        Store.Drained drained = store.drain(selected);
        Messages.Clock clock = clock();
        List<Store.Row> rows = null;

        for (Map.Entry<Client, Session> e : sessions.entrySet()) {
            Client client = e.getKey();
            Session s = e.getValue();
            if (client.pendingChars() > MAX_PENDING_CHARS) {
                sessions.remove(client);
                client.close("too far behind");
                continue;
            }
            String symbol = s.selected;
            if (s.needsSnapshot) {
                s.needsSnapshot = false;
                s.needsSeries = false;
                if (rows == null) {
                    rows = store.rows();
                }
                client.send(Messages.snapshot(store.latestWindowEnd(), rows,
                        symbol == null ? null : store.series(symbol), clock));
                remember(s, clock);
                continue;
            }
            if (s.needsSeries && symbol != null) {
                s.needsSeries = false;
                client.send(Messages.series(store.series(symbol)));
            }
            List<Store.Point> points = symbol == null ? null : drained.points().get(symbol);
            boolean clockMoved = !Objects.equals(s.sentFeedMs, clock.feedMs())
                    || !Objects.equals(s.sentIdleSince, clock.feedIdleSince())
                    || !Objects.equals(s.sentResultsMs, clock.resultsMs());
            if (!drained.cells().isEmpty() || points != null || clockMoved) {
                client.send(Messages.delta(drained.spanEnd(), drained.cells(), symbol, points, clock));
                remember(s, clock);
            }
        }
    }

    Messages.Clock clock() {
        long wall = wallClock.getAsLong();
        return new Messages.Clock(wall, feed.feedMs(), feed.idleSince(wall), store.latestWindowEnd(), zoneOffset);
    }

    private static void remember(Session s, Messages.Clock clock) {
        s.sentFeedMs = clock.feedMs();
        s.sentIdleSince = clock.feedIdleSince();
        s.sentResultsMs = clock.resultsMs();
    }
}
