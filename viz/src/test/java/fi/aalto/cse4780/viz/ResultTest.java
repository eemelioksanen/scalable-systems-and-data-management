package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ResultTest {

    @Test
    void parsesTheInterfacesExample() {
        Result r = Result.parse("{\"symbol\":\"RDSA.NL\",\"secType\":\"E\",\"windowEnd\":1636355100000,"
                + "\"close\":19.28,\"ema38\":19.2712,\"ema100\":19.2655,"
                + "\"maxIngestTs\":1790000000123,\"emitTs\":1790000000456}");
        assertEquals(new Result("RDSA.NL", "E", 1636355100000L, 19.28, 19.2712, 19.2655), r);
    }

    @Test
    void ignoresUnknownFields() {
        Result r = Result.parse("{\"symbol\":\"846741.ETR\",\"secType\":\"I\",\"windowEnd\":1,\"close\":1,"
                + "\"ema38\":0.5,\"ema100\":0.25,\"currency\":\"EUR\"}");
        assertEquals("846741.ETR", r.symbol());
        assertEquals("ETR", Result.exchangeOf(r.symbol()));
    }

    @Test
    void rejectsMalformedValues() {
        assertNull(Result.parse(null));
        assertNull(Result.parse("not json"));
        assertNull(Result.parse("[1,2]"));
        assertNull(Result.parse("{\"symbol\":\"A.FR\",\"secType\":\"E\",\"windowEnd\":1,\"close\":1,\"ema38\":1}"));
        assertNull(Result.parse("{\"symbol\":\"A.FR\",\"secType\":\"E\",\"windowEnd\":\"x\",\"close\":1,\"ema38\":1,\"ema100\":1}"));
        assertNull(Result.parse("{\"symbol\":\"\",\"secType\":\"E\",\"windowEnd\":1,\"close\":1,\"ema38\":1,\"ema100\":1}"));
    }
}
