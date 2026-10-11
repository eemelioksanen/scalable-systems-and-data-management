package fi.aalto.cse4780.viz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/** The WebSocket messages of viz/README.md. */
final class Messages {

    private static final ObjectMapper JSON = new ObjectMapper();

    record Clock(long wallMs, Long feedMs, Long feedIdleSince, Long resultsMs, String zoneOffset) {}

    private Messages() {}

    static String snapshot(Long spanEnd, List<Store.Row> rows, Store.Series series, Clock clock) {
        ObjectNode root = JSON.createObjectNode();
        root.put("type", "snapshot");
        putLong(root, "spanEnd", spanEnd);
        root.put("slots", Store.SLOTS);
        root.put("slotMs", Store.SLOT_MS);
        ArrayNode symbols = root.putArray("symbols");
        for (Store.Row row : rows) {
            ObjectNode o = symbols.addObject();
            o.put("symbol", row.symbol());
            o.put("secType", row.secType());
            o.put("exchange", row.exchange());
            putLong(o, "lastFlip", row.lastFlip());
            o.put("cells", row.cells());
        }
        if (series == null) {
            root.putNull("series");
        } else {
            root.set("series", seriesNode(series));
        }
        root.set("clock", clockNode(clock));
        return root.toString();
    }

    static String series(Store.Series series) {
        ObjectNode root = JSON.createObjectNode();
        root.put("type", "series");
        root.set("series", seriesNode(series));
        return root.toString();
    }

    static String delta(Long spanEnd, List<Store.Cell> cells, String pointsSymbol, List<Store.Point> points, Clock clock) {
        ObjectNode root = JSON.createObjectNode();
        root.put("type", "delta");
        putLong(root, "spanEnd", spanEnd);
        ArrayNode cellArray = root.putArray("cells");
        for (Store.Cell c : cells) {
            ArrayNode a = cellArray.addArray();
            a.add(c.symbol());
            a.add(c.windowEnd());
            a.add(String.valueOf(c.sign()));
            a.add(c.secType());
            a.add(c.exchange());
            if (c.lastFlip() == null) {
                a.addNull();
            } else {
                a.add(c.lastFlip());
            }
        }
        if (points == null) {
            root.putNull("points");
        } else {
            ObjectNode p = root.putObject("points");
            p.put("symbol", pointsSymbol);
            p.set("points", pointArray(points));
        }
        root.set("clock", clockNode(clock));
        return root.toString();
    }

    private static ObjectNode seriesNode(Store.Series series) {
        ObjectNode o = JSON.createObjectNode();
        o.put("symbol", series.symbol());
        putLong(o, "firstWindowEnd", series.firstWindowEnd());
        o.set("points", pointArray(series.points()));
        return o;
    }

    private static ArrayNode pointArray(List<Store.Point> points) {
        ArrayNode out = JSON.createArrayNode();
        for (Store.Point p : points) {
            out.addArray().add(p.windowEnd()).add(p.close()).add(p.ema38()).add(p.ema100());
        }
        return out;
    }

    private static ObjectNode clockNode(Clock clock) {
        ObjectNode o = JSON.createObjectNode();
        o.put("wallMs", clock.wallMs());
        putLong(o, "feedMs", clock.feedMs());
        putLong(o, "feedIdleSince", clock.feedIdleSince());
        putLong(o, "resultsMs", clock.resultsMs());
        o.put("zoneOffset", clock.zoneOffset());
        return o;
    }

    private static void putLong(ObjectNode o, String field, Long value) {
        if (value == null) {
            o.putNull(field);
        } else {
            o.put(field, value.longValue());
        }
    }
}
