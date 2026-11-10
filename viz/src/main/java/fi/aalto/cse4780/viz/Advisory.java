package fi.aalto.cse4780.viz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One {@code advisories} record (docs/interfaces.md, D-16). {@code maxIngestTs} and {@code emitTs}
 * give the advisory's latency from admission to publication (D-40); either may be null.
 */
record Advisory(String symbol, String secType, long windowEnd, String type, double ema38, double ema100,
        Long maxIngestTs, Long emitTs) {

    Advisory(String symbol, String secType, long windowEnd, String type, double ema38, double ema100) {
        this(symbol, secType, windowEnd, type, ema38, ema100, null, null);
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The record, or null if the value is not JSON, lacks a field, or its type is not BUY or SELL. */
    static Advisory parse(String value) {
        if (value == null) {
            return null;
        }
        JsonNode node;
        try {
            node = JSON.readTree(value);
        } catch (Exception e) {
            return null;
        }
        if (node == null || !node.isObject()) {
            return null;
        }
        JsonNode symbol = node.get("symbol");
        JsonNode secType = node.get("secType");
        JsonNode windowEnd = node.get("windowEnd");
        JsonNode type = node.get("type");
        JsonNode ema38 = node.get("ema38");
        JsonNode ema100 = node.get("ema100");
        if (symbol == null || !symbol.isTextual() || symbol.asText().isEmpty()
                || secType == null || !secType.isTextual()
                || windowEnd == null || !windowEnd.canConvertToLong()
                || type == null || !(type.asText().equals("BUY") || type.asText().equals("SELL"))
                || ema38 == null || !ema38.isNumber()
                || ema100 == null || !ema100.isNumber()) {
            return null;
        }
        return new Advisory(symbol.asText(), secType.asText(), windowEnd.asLong(), type.asText(),
                ema38.asDouble(), ema100.asDouble(), optionalLong(node.get("maxIngestTs")),
                optionalLong(node.get("emitTs")));
    }

    private static Long optionalLong(JsonNode n) {
        return n != null && n.canConvertToLong() ? n.asLong() : null;
    }

    String exchange() {
        return Result.exchangeOf(symbol);
    }
}
