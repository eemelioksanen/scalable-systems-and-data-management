package fi.aalto.cse4780.viz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Every trader's current symbol list, from the compacted {@code subscriptions} topic (D-17). The
 * same rules as the job: the latest list replaces the previous one, a tombstone removes the
 * trader, and anything else is malformed and ignored.
 */
final class Subscriptions {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The lists at one version; the version increases with every change. */
    record View(long version, Map<String, List<String>> traders) {}

    private final Map<String, List<String>> traders = new TreeMap<>();
    private long version;

    /** Applies one record. Returns false if it is malformed. */
    synchronized boolean apply(String trader, String value) {
        if (trader == null || trader.isEmpty()) {
            return false;
        }
        if (value == null) {
            if (traders.remove(trader) != null) {
                version++;
            }
            return true;
        }
        List<String> symbols = parse(trader, value);
        if (symbols == null) {
            return false;
        }
        if (!symbols.equals(traders.put(trader, symbols))) {
            version++;
        }
        return true;
    }

    /** A copy of the current lists, by trader, with their version. */
    synchronized View view() {
        return new View(version, new TreeMap<>(traders));
    }

    synchronized int size() {
        return traders.size();
    }

    private static List<String> parse(String trader, String value) {
        JsonNode node;
        try {
            node = JSON.readTree(value);
        } catch (Exception e) {
            return null;
        }
        if (node == null || !node.isObject()) {
            return null;
        }
        JsonNode named = node.get("trader");
        if (named != null && !(named.isTextual() && named.asText().equals(trader))) {
            return null;
        }
        JsonNode symbols = node.get("symbols");
        if (symbols == null || !symbols.isArray()) {
            return null;
        }
        TreeSet<String> out = new TreeSet<>();
        for (JsonNode s : symbols) {
            if (!s.isTextual() || s.asText().isEmpty()) {
                return null;
            }
            out.add(s.asText());
        }
        return new ArrayList<>(out);
    }
}
