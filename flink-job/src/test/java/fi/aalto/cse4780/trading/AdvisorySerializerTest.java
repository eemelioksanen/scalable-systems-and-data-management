package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class AdvisorySerializerTest {

    private static EmaResult result(Crossover crossover) {
        EmaResult result = new EmaResult("RDSA.NL", "E", 1_636_362_300_000L,
                19.28, 5.128205128205129, 1.9801980198019802, 1_790_000_000_123L, 1_790_000_000_456L);
        result.crossover = crossover;
        return result;
    }

    @Test
    void writesTheAgreedFieldsInOrder() {
        assertEquals(
                "{\"symbol\":\"RDSA.NL\",\"secType\":\"E\",\"windowEnd\":1636362300000,"
                        + "\"type\":\"BUY\",\"ema38\":5.128205128205129,\"ema100\":1.9801980198019802,"
                        + "\"maxIngestTs\":1790000000123,\"emitTs\":1790000000456}",
                AdvisorySerializer.toJson(result(Crossover.BUY)));
        assertEquals(
                "{\"symbol\":\"RDSA.NL\",\"secType\":\"E\",\"windowEnd\":1636362300000,"
                        + "\"type\":\"SELL\",\"ema38\":5.128205128205129,\"ema100\":1.9801980198019802,"
                        + "\"maxIngestTs\":1790000000123,\"emitTs\":1790000000456}",
                AdvisorySerializer.toJson(result(Crossover.SELL)));
    }

    @Test
    void keysRecordsBySymbol() {
        EmaResult buy = result(Crossover.BUY);
        ProducerRecord<byte[], byte[]> record =
                new AdvisorySerializer("advisories").serialize(buy, null, null);

        assertEquals("advisories", record.topic());
        assertArrayEquals("RDSA.NL".getBytes(StandardCharsets.UTF_8), record.key());
        assertEquals(AdvisorySerializer.toJson(buy),
                new String(record.value(), StandardCharsets.UTF_8));
    }
}
