package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DashboardOptionsTest {

    @Test
    void defaultsMatchTheJobAndCompose() {
        DashboardOptions o = DashboardOptions.parse(new String[0], Map.of("BOOTSTRAP_SERVERS", "kafka:9092"));
        assertEquals("kafka:9092", o.bootstrapServers());
        assertEquals(8088, o.port());
        assertEquals(ZoneOffset.of("+02:00"), o.zoneOffset());
        assertEquals("ema-results", o.resultsTopic());
        assertEquals("ticks", o.ticksTopic());
        assertEquals("advisories", o.advisoriesTopic());
        assertEquals("subscriptions", o.subscriptionsTopic());
    }

    @Test
    void parsesFlags() {
        DashboardOptions o = DashboardOptions.parse(new String[] {
            "--port", "0", "--zone-offset", "Z", "--results-topic", "r", "--flush-ms", "50"}, Map.of());
        assertEquals("localhost:29092", o.bootstrapServers());
        assertEquals(0, o.port());
        assertEquals(ZoneOffset.UTC, o.zoneOffset());
        assertEquals(50, o.flushMs());
    }

    @Test
    void rejectsBadInput() {
        assertThrows(IllegalArgumentException.class, () -> DashboardOptions.parse(new String[] {"--port"}, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> DashboardOptions.parse(new String[] {"--nope", "1"}, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> DashboardOptions.parse(new String[] {"--zone-offset", "CEST"}, Map.of()));
    }
}
