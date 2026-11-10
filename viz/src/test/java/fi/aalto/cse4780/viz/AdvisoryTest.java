package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class AdvisoryTest {

    @Test
    void parsesTheInterfacesExample() {
        Advisory a = Advisory.parse("{\"symbol\":\"RDSA.NL\",\"secType\":\"E\",\"windowEnd\":1636355100000,"
                + "\"type\":\"BUY\",\"ema38\":19.2712,\"ema100\":19.2655,\"maxIngestTs\":1790000000123,"
                + "\"emitTs\":1790000000456}");
        assertEquals(new Advisory("RDSA.NL", "E", 1636355100000L, "BUY", 19.2712, 19.2655), a);
        assertEquals("NL", a.exchange());
    }

    @Test
    void rejectsMalformedRecords() {
        assertNull(Advisory.parse(null));
        assertNull(Advisory.parse("nope"));
        assertNull(Advisory.parse("{\"symbol\":\"A.FR\",\"secType\":\"E\",\"windowEnd\":1,\"ema38\":1,\"ema100\":2}"));
        assertNull(Advisory.parse("{\"symbol\":\"A.FR\",\"secType\":\"E\",\"windowEnd\":1,\"type\":\"buy\","
                + "\"ema38\":1,\"ema100\":2}"));
    }
}
