package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class EmaResultSerializerTest {

    private static final EmaResult RESULT = new EmaResult("RDSA.NL", "E", 1_636_362_300_000L,
            19.28, 5.128205128205129, 1.9801980198019802, 1_790_000_000_123L, 1_790_000_000_456L);

    @Test
    void writesTheAgreedFieldsInOrder() {
        assertEquals(
                "{\"symbol\":\"RDSA.NL\",\"secType\":\"E\",\"windowEnd\":1636362300000,"
                        + "\"close\":19.28,\"ema38\":5.128205128205129,\"ema100\":1.9801980198019802,"
                        + "\"maxIngestTs\":1790000000123,\"emitTs\":1790000000456}",
                EmaResultSerializer.toJson(RESULT));
    }

    @Test
    void keysRecordsBySymbol() {
        ProducerRecord<byte[], byte[]> record =
                new EmaResultSerializer("ema-results").serialize(RESULT, null, null);

        assertEquals("ema-results", record.topic());
        assertArrayEquals("RDSA.NL".getBytes(StandardCharsets.UTF_8), record.key());
        assertEquals(EmaResultSerializer.toJson(RESULT),
                new String(record.value(), StandardCharsets.UTF_8));
    }

    @Test
    void leavesTheCrossoverToTheAdvisoriesTopic() {
        EmaResult withCrossover = new EmaResult("RDSA.NL", "E", 1_636_362_300_000L,
                19.28, 5.128205128205129, 1.9801980198019802, 1_790_000_000_123L, 1_790_000_000_456L);
        withCrossover.crossover = Crossover.BUY;

        assertEquals(EmaResultSerializer.toJson(RESULT), EmaResultSerializer.toJson(withCrossover));
    }

    @Test
    void escapesStrings() {
        EmaResult odd = new EmaResult("A\"B\\C\n", null, 0, 1, 2, 3, 4, 5);

        assertEquals(
                "{\"symbol\":\"A\\\"B\\\\C\\u000a\",\"secType\":null,\"windowEnd\":0,"
                        + "\"close\":1.0,\"ema38\":2.0,\"ema100\":3.0,\"maxIngestTs\":4,\"emitTs\":5}",
                EmaResultSerializer.toJson(odd));
    }
}
