package fi.aalto.cse4780.viz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** One {@code ema-results} record (docs/interfaces.md, D-16), without the latency fields. */
record Result(String symbol, String secType, long windowEnd, double close, double ema38, double ema100) {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The record, or null if the value is not JSON or lacks a field. */
    static Result parse(String value) {
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
        JsonNode close = node.get("close");
        JsonNode ema38 = node.get("ema38");
        JsonNode ema100 = node.get("ema100");
        if (symbol == null || !symbol.isTextual() || symbol.asText().isEmpty()
                || secType == null || !secType.isTextual()
                || windowEnd == null || !windowEnd.canConvertToLong()
                || close == null || !close.isNumber()
                || ema38 == null || !ema38.isNumber()
                || ema100 == null || !ema100.isNumber()) {
            return null;
        }
        return new Result(symbol.asText(), secType.asText(), windowEnd.asLong(),
                close.asDouble(), ema38.asDouble(), ema100.asDouble());
    }

    /** The exchange is the ID suffix (D-1). */
    static String exchangeOf(String symbol) {
        int dot = symbol.lastIndexOf('.');
        return dot < 0 ? "" : symbol.substring(dot + 1);
    }
}
