package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class LateTickSerializerTest {

    @Test
    void writesSourceIdentityAndExclusiveWindowEnd() throws Exception {
        Tick tick = new Tick("S\".NL", "E", 12.0, 300_000, 9, 10);
        tick.sourceFileIndex = 2;
        tick.sourceLine = 17L;
        var record = new LateTickSerializer("rejected").serialize(tick, null, null);
        var json = new ObjectMapper().readTree(record.value());
        assertEquals("rejected", record.topic());
        assertEquals(tick.symbol, new String(record.key(), StandardCharsets.UTF_8));
        assertEquals(tick.symbol, json.get("symbol").asText());
        assertEquals(600_000L, json.get("windowEnd").asLong());
        assertEquals(2, json.get("sourceFileIndex").asInt());
        assertEquals(17L, json.get("sourceLine").asLong());
    }

    @Test
    void preservesMissingProvenanceAsNull() throws Exception {
        Tick tick = new Tick("S.NL", "E", 12.0, 0, 9, 10);
        var record = new LateTickSerializer("late-ticks").serialize(tick, null, null);
        var json = new ObjectMapper().readTree(record.value());
        assertTrue(json.get("sourceFileIndex").isNull());
        assertTrue(json.get("sourceLine").isNull());
    }
}
