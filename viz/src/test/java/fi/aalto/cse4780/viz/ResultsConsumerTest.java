package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Test;

class ResultsConsumerTest {

    static String json(String symbol, long windowEnd, double ema38, double ema100) {
        return "{\"symbol\":\"" + symbol + "\",\"secType\":\"E\",\"windowEnd\":" + windowEnd
                + ",\"close\":10.0,\"ema38\":" + ema38 + ",\"ema100\":" + ema100
                + ",\"maxIngestTs\":1,\"emitTs\":2}";
    }

    @Test
    void readsEveryPartitionIntoTheStoreAndCountsMalformedRecords() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("ema-results", 2);
        Store store = new Store();
        ResultsConsumer results = new ResultsConsumer(consumer, "ema-results", store);

        assertTrue(results.pollOnce(Duration.ZERO));
        consumer.addRecord(KafkaFixtures.record("ema-results", 0, 0, 0, json("A.FR", StoreTest.T0, 2, 1)));
        consumer.addRecord(KafkaFixtures.record("ema-results", 1, 0, 0, json("B.NL", StoreTest.T0, 1, 2)));
        consumer.addRecord(KafkaFixtures.record("ema-results", 1, 1, 0, "garbage"));
        results.pollOnce(Duration.ZERO);

        assertEquals(2, results.resultsRead());
        assertEquals(1, results.malformedResults());
        assertEquals(2, store.symbolCount());
    }

    @Test
    void waitsWhileTheTopicDoesNotExist() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("other", 1);
        ResultsConsumer results = new ResultsConsumer(consumer, "ema-results", new Store());

        assertFalse(results.pollOnce(Duration.ZERO));
    }
}
